package org.example.worker; // Package for worker-related classes

import org.example.bootstrap.BootstrapService; // To register with bootstrap node
import org.example.jobs.Job;                   // Job data structure
import org.example.jobs.JobResult;             // Job result structure
import org.example.jobs.JobUtils;              // Shared data-shape + isPrime helpers
import org.example.leader.LeaderNode;          // Job splitting/combining helper (coordinator side)
import java.rmi.registry.LocateRegistry;       // To find RMI registry
import java.rmi.registry.Registry;             // Registry reference
import java.rmi.server.UnicastRemoteObject;    // Base class for RMI objects
import java.rmi.RemoteException;               // Exception for RMI methods
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// WorkerNode implements WorkerInterface so it can be called remotely
public class WorkerNode extends UnicastRemoteObject implements WorkerInterface {

    // Required by the assignment spec — a marker field on every worker node.
    public static final String leaderman = "cs324";

    // How long a node waits, after joining an ELECTION flood, before deciding
    // whether *it* was the winner. Must be long enough for the flood to reach
    // every node in the connected component (bounded by network diameter).
    private static final long ELECTION_TIMEOUT_MS = 3000;

    // How often a worker pings its known coordinator to detect failure.
    private static final long HEARTBEAT_INTERVAL_MS = 2000;

    private final int id;          // Unique worker ID
    private volatile int jac = 0;  // Job Allocation Counter (jobs assigned while coordinator, cumulative)
    private volatile int jobsThisTerm = 0; // Jobs assigned during the CURRENT coordinator term (resets each term)

    // The unstructured network: this worker's direct neighbors only.
    private final List<WorkerInterface> neighbors = new CopyOnWriteArrayList<>();

    // Dedup set for COORDINATOR messages — those carry a single already-decided
    // value with nothing to compare/improve, so simple "process once" dedup
    // is correct and sufficient there.
    private final Set<String> seenCoordinatorMessages = ConcurrentHashMap.newKeySet();

    // Per-election-in-flight bookkeeping: for a given messageId, the best
    // (id, jac) candidate this node currently believes is the winner, stored
    // as {id, jac}. IMPORTANT: unlike the coordinator flood, this is NOT a
    // simple "seen it once, drop duplicates" dedup — an ELECTION message can
    // legitimately be re-processed by this node if new information arrives
    // that improves on what it previously held, and it will then re-forward
    // the improved value. This is what guarantees the algorithm converges to
    // one single global winner on any connected graph (see design note on
    // onElectionMessage below). It still always terminates: a node's locally
    // held best only ever gets better, never worse, and there's a finite
    // number of workers, so the number of updates per node is bounded.
    private final ConcurrentHashMap<String, int[]> bestCandidate = new ConcurrentHashMap<>();

    private volatile int coordinatorId = -1;               // -1 = no known coordinator
    private volatile boolean isCoordinator = false;
    private volatile WorkerInterface coordinatorStub = null; // live callable reference to the coordinator

    // Reference to the bootstrap node — needed so the coordinator can look up
    // the currently active worker set to distribute jobs across, and so a
    // freshly-elected coordinator can tell bootstrap "it's me now".
    private volatile BootstrapService bootstrap;

    private final ExecutorService messageExecutor = Executors.newCachedThreadPool(); // async neighbor calls + job dispatch
    // Worker-side pool that actually RUNS jobs. Fixed size (>= 4, or one per CPU core),
    // created in the constructor so its threads can be named after this worker.
    private final ExecutorService jobExecutor;
    private final AtomicInteger activeJobs = new AtomicInteger(); // jobs running right now (for logging)
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // timeouts + heartbeat

    // Constructor: sets worker ID and exports object for RMI
    public WorkerNode(int id) throws RemoteException {
        super(); // Calls UnicastRemoteObject constructor
        this.id = id;
        int poolSize = Math.max(4, Runtime.getRuntime().availableProcessors());
        AtomicInteger threadCounter = new AtomicInteger();
        this.jobExecutor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread thread = new Thread(r, "worker-" + id + "-job-" + threadCounter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        startHeartbeatMonitor(); // begin watching the coordinator (once one exists)
    }

    // Returns worker ID (used in leader election and logging)
    @Override
    public int getId() { return id; }

    // Returns current JAC value (used in leader election)
    @Override
    public int getJobAllocationCounter() { return jac; }

    public boolean isCoordinator() { return isCoordinator; }
    public int getCoordinatorId() { return coordinatorId; }

    // Must be called once after construction (see main()) so this worker can
    // reach bootstrap for coordinator lookups / announcements.
    public void setBootstrap(BootstrapService bootstrap) {
        this.bootstrap = bootstrap;
    }

    // Directly asks Bootstrap who the current coordinator is, rather than
    // relying solely on catching a COORDINATOR flood in flight. This closes
    // a real gap: a worker joining AFTER a flood has already fully finished
    // propagating would otherwise never find out who's coordinator until the
    // next election (5-job term limit or a heartbeat failure) — it would sit
    // there blind in the meantime. Safe to call even if no coordinator has
    // been elected yet (bootstrap.getCoordinator() just returns null).
    public void adoptCoordinatorFromBootstrap() {
        if (bootstrap == null) return;
        try {
            WorkerInterface coord = bootstrap.getCoordinator();
            if (coord != null) {
                int coordId = coord.getId(); // also doubles as a liveness check
                this.coordinatorStub = coord;
                this.coordinatorId = coordId;
                this.isCoordinator = (coordId == this.id);
                System.out.println("[Worker " + id + "] Learned current coordinator from Bootstrap: Worker " + coordId);
            }
        } catch (RemoteException e) {
            System.out.println("[Worker " + id + "] Bootstrap's coordinator reference looked stale: " + e.getMessage());
        }
    }

    // ============================================================
    //                    JOB EXECUTION (worker side)
    // ============================================================

    // Executes a job sent by the coordinator. Every call runs on this worker's own
    // thread pool, so several jobs (chunks from the coordinator, or jobs from
    // different clients) execute AT THE SAME TIME on separate Java threads.
    // The log lines show the thread name and how many jobs are running right now,
    // which makes the concurrency visible in a demo.
    @Override
    public JobResult executeJob(Job job) throws RemoteException {
        try {
            Future<JobResult> future = jobExecutor.submit(() -> {
                int running = activeJobs.incrementAndGet();
                String thread = Thread.currentThread().getName();
                System.out.println("[Worker " + id + "] START " + job.getType()
                        + " on " + thread + " (jobs running now: " + running + ")");
                try {
                    return computeJob(job);
                } finally {
                    int left = activeJobs.decrementAndGet();
                    System.out.println("[Worker " + id + "] DONE  " + job.getType()
                            + " on " + thread + " (jobs still running: " + left + ")");
                }
            });
            return future.get();
        } catch (Exception e) {
            throw new RemoteException("Worker " + id + " failed executing job " + job.getType(), e);
        }
    }

    private JobResult computeJob(Job job) {
        switch (job.getType()) {
            case "MAX": {
                int[] nums = JobUtils.toIntArray(job.getData());
                if (nums.length == 0) {
                    // Sentinel so this chunk never wins the MAX comparison.
                    return new JobResult("Worker " + id + ": MAX of empty chunk", Long.MIN_VALUE);
                }
                int max = nums[0];
                for (int n : nums) {
                    if (n > max) max = n;
                }
                return new JobResult("Worker " + id + " handled " + nums.length + " numbers -> max " + max, max);
            }
            case "PRIMESUM": {
                int[] range = JobUtils.toIntArray(job.getData());
                int start = range[0];
                int end = range[1];
                long sum = 0;
                for (int n = start; n <= end; n++) {
                    if (JobUtils.isPrime(n)) sum += n;
                }
                return new JobResult("Worker " + id + " handled range " + start + "-" + end + " -> prime sum " + sum, sum);
            }
            case "PRIMECOUNT": {
                int[] nums = JobUtils.toIntArray(job.getData());
                long count = 0;
                for (int n : nums) {
                    if (JobUtils.isPrime(n)) count++;
                }
                return new JobResult("Worker " + id + " handled " + nums.length + " numbers -> " + count + " primes", count);
            }
            default:
                return new JobResult("Worker " + id + ": unknown job type " + job.getType(), 0);
        }
    }

    // ============================================================
    //              JOB SUBMISSION / DISTRIBUTION (coordinator side)
    // ============================================================

    // Client-facing entry point. Only works if this worker is currently the
    // elected coordinator. Clients should obtain the right stub to call this
    // on via BootstrapService.getCoordinator().
    @Override
    public JobResult submitJob(Job job) throws RemoteException {
        if (!isCoordinator) {
            throw new RemoteException("Worker " + id + " is not the coordinator right now "
                    + "(current coordinator: " + coordinatorId + "). Ask Bootstrap for the current coordinator.");
        }
        if (bootstrap == null) {
            throw new RemoteException("Coordinator has no bootstrap reference; cannot look up active workers.");
        }

        // 1) Only count workers that answer RIGHT NOW, so the job is split evenly
        //    across the workers that will really do the work (not across dead ones).
        List<WorkerInterface> workers = new ArrayList<>();
        List<Integer> workerIds = new ArrayList<>();
        for (WorkerInterface w : bootstrap.getWorkers()) {
            try {
                int wid = w.getId();
                workers.add(w);
                workerIds.add(wid);
            } catch (RemoteException e) {
                System.out.println("[Worker " + id + " | COORDINATOR] Skipping unreachable worker: " + e.getMessage());
            }
        }
        if (workers.isEmpty()) {
            throw new RemoteException("No active workers available to run this job.");
        }

        List<Job> subJobs = LeaderNode.splitJob(job, workers.size());
        System.out.println("[Worker " + id + " | COORDINATOR] Distributing " + job.getType()
                + " into " + subJobs.size() + " sub-jobs across " + workers.size() + " workers.");

        // 2) Dispatch every chunk at once; each worker runs its chunk in parallel.
        List<Future<JobResult>> futures = new ArrayList<>();
        int assignedToOthers = 0;
        for (int i = 0; i < subJobs.size(); i++) {
            WorkerInterface target = workers.get(i);
            Job subJob = subJobs.get(i);
            futures.add(messageExecutor.submit(() -> target.executeJob(subJob)));
            if (workerIds.get(i) != this.id) {
                assignedToOthers++; // per spec: JAC counts jobs given to OTHER workers
            }
        }

        // 3) Collect. If a worker dies mid-job, its chunk is re-run here rather than
        //    being dropped, so the combined answer is always complete.
        List<JobResult> partials = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                partials.add(futures.get(i).get());
            } catch (Exception e) {
                System.out.println("[Worker " + id + " | COORDINATOR] A worker failed its chunk; re-running it locally.");
                partials.add(executeJob(subJobs.get(i)));
            }
        }
        JobResult combined = LeaderNode.combineResults(job.getType(), partials);
        StringBuilder breakdown = new StringBuilder(combined.getResult());
        breakdown.append("\n  Coordinator Worker ").append(id).append(" split this job across ")
                .append(partials.size()).append(" workers:");
        for (JobResult part : partials) {
            breakdown.append("\n    - ").append(part.getResult());
        }
        combined = new JobResult(breakdown.toString(), combined.getValue());

        // 4) JAC bookkeeping happens AFTER the job is finished, so a term ending
        //    (and a new election starting) can never interrupt a job in progress.
        if (assignedToOthers > 0) {
            recordJobsAssigned(assignedToOthers);
        }
        return combined;
    }

    // ============================================================
    //                       LEADER ELECTION
    // ============================================================

    // Comparison rule required by the assignment:
    // lowest JAC wins; on a tie, highest ID wins.
    // Returns true if (candidateId, candidateJac) is strictly better than
    // (currentBestId, currentBestJac).
    private boolean isBetterCandidate(int candidateId, int candidateJac,
                                      int currentBestId, int currentBestJac) {
        if (candidateJac != currentBestJac) {
            return candidateJac < currentBestJac;
        }
        return candidateId > currentBestId;
    }

    // Any worker may call this on itself to start a new election, e.g. when
    // it notices there is no active coordinator.
    @Override
    public void startElection() {
        String messageId = "elec-" + id + "-" + UUID.randomUUID();
        System.out.println("[Worker " + id + "] Starting election " + messageId
                + " (my JAC=" + jac + ")");

        // No coordinator while an election is in progress.
        coordinatorId = -1;
        isCoordinator = false;
        coordinatorStub = null;

        bestCandidate.put(messageId, new int[]{id, jac});

        broadcastElection(messageId, id, jac);

        // Give the flood time to reach everyone, then check if I won.
        scheduler.schedule(() -> concludeElection(messageId), ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    // Invoked remotely by a neighbor forwarding an in-progress ELECTION flood.
    //
    // DESIGN NOTE: this is intentionally NOT "process this messageId once,
    // ever, then ignore it forever" — that simpler rule has a real bug on
    // general graphs: if a node forwards its own value, then later learns
    // (via a different path) of a strictly better candidate, dropping that
    // update means the better value never reaches the rest of the network
    // through this node. Two isolated "leaf" nodes attached to the same hub
    // can each end up believing they're the winner, with neither ever
    // learning about the other — which is exactly the multi-coordinator bug
    // this fixes. Instead: a node only re-forwards (and reschedules its own
    // conclusion check) when the incoming information actually IMPROVES what
    // it's currently holding for this messageId. That still can't loop
    // forever — a candidate can only get "better" a bounded number of times
    // before it reaches the true global best — but it does let good
    // information propagate back out through nodes that already forwarded once.
    @Override
    public void onElectionMessage(String messageId, int candidateId, int candidateJac) {
        boolean improved = updateBestCandidate(messageId, candidateId, candidateJac);

        if (!improved) {
            return; // nothing new for this node to relay
        }

        int[] best = bestCandidate.get(messageId);
        System.out.println("[Worker " + id + "] ELECTION " + messageId
                + " update -> best so far is Worker" + best[0] + " (JAC=" + best[1] + ")");

        broadcastElection(messageId, best[0], best[1]);

        scheduler.schedule(() -> concludeElection(messageId), ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    // Atomically merges an incoming (candidateId, candidateJac) into this
    // node's locally-held best for `messageId`, folding in this node's own
    // (id, jac) the first time it touches this message. Returns true if the
    // locally-held best changed as a result (including the very first touch,
    // since this node must still forward at least once to participate in
    // the flood) — the caller should re-broadcast only when this is true.
    private boolean updateBestCandidate(String messageId, int candidateId, int candidateJac) {
        boolean[] changed = {false};
        bestCandidate.compute(messageId, (key, existing) -> {
            int baseId, baseJac;
            boolean firstTime = (existing == null);
            if (firstTime) {
                baseId = this.id;
                baseJac = this.jac;
            } else {
                baseId = existing[0];
                baseJac = existing[1];
            }

            int newId = baseId;
            int newJac = baseJac;
            if (isBetterCandidate(candidateId, candidateJac, newId, newJac)) {
                newId = candidateId;
                newJac = candidateJac;
            }

            if (firstTime || newId != baseId || newJac != baseJac) {
                changed[0] = true;
            }
            return new int[]{newId, newJac};
        });
        return changed[0];
    }

    // Sends the given (candidateId, candidateJac) ELECTION payload to every
    // known neighbor, asynchronously and fault-tolerantly (dead neighbors are
    // dropped from the topology rather than blocking the flood).
    private void broadcastElection(String messageId, int candidateId, int candidateJac) {
        for (WorkerInterface neighbor : neighbors) {
            messageExecutor.submit(() -> {
                try {
                    neighbor.onElectionMessage(messageId, candidateId, candidateJac);
                } catch (RemoteException e) {
                    System.out.println("[Worker " + id + "] Neighbor unreachable during election, dropping it: "
                            + e.getMessage());
                    neighbors.remove(neighbor);
                }
            });
        }
    }

    // After the flood has had time to settle, check whether I ended up as
    // the best candidate for this election. If so, announce myself.
    private void concludeElection(String messageId) {
        int[] best = bestCandidate.get(messageId);
        if (best == null) {
            return; // shouldn't happen, but be defensive
        }
        if (best[0] == this.id && !this.isCoordinator) {
            System.out.println("[Worker " + id + "] Won election " + messageId + " -> announcing coordinator.");
            announceCoordinator();
        }
    }

    // ============================================================
    //                    COORDINATOR ANNOUNCEMENT
    // ============================================================

    // Called by the winning worker on itself once it has determined (via
    // concludeElection) that it has the best (JAC, ID) pair.
    public void announceCoordinator() {
        this.coordinatorId = this.id;
        this.isCoordinator = true;
        this.coordinatorStub = this;
        this.jobsThisTerm = 0; // fresh term: up to 5 jobs before the next election

        String messageId = "coord-" + id + "-" + UUID.randomUUID();
        seenCoordinatorMessages.add(messageId);

        System.out.println("[Worker " + id + "] === NEW COORDINATOR: Worker " + id
                + " (JAC=" + jac + ", leaderman=" + leaderman + ") ===");

        broadcastCoordinator(messageId, this, this.id, this.jac);

        // Tell bootstrap directly too, so clients can discover the
        // coordinator even if they aren't reachable by the flood themselves.
        if (bootstrap != null) {
            try {
                bootstrap.updateCoordinator(this, this.id, this.jac);
            } catch (RemoteException e) {
                System.out.println("[Worker " + id + "] Could not notify bootstrap of new coordinator: " + e.getMessage());
            }
        }
    }

    // Invoked remotely by a neighbor forwarding an in-progress COORDINATOR flood.
    @Override
    public void onCoordinatorMessage(String messageId, WorkerInterface coordinator,
                                     int coordId, int coordJac) {
        if (!seenCoordinatorMessages.add(messageId)) {
            return; // already processed this exact coordinator message
        }

        this.coordinatorId = coordId;
        this.isCoordinator = (coordId == this.id);
        this.coordinatorStub = coordinator; // live stub, usable even multiple hops away

        System.out.println("[Worker " + id + "] Learned coordinator: Worker " + coordId
                + " (JAC=" + coordJac + ")");

        broadcastCoordinator(messageId, coordinator, coordId, coordJac);
    }

    private void broadcastCoordinator(String messageId, WorkerInterface coordinator,
                                      int coordId, int coordJac) {
        for (WorkerInterface neighbor : neighbors) {
            messageExecutor.submit(() -> {
                try {
                    neighbor.onCoordinatorMessage(messageId, coordinator, coordId, coordJac);
                } catch (RemoteException e) {
                    System.out.println("[Worker " + id + "] Neighbor unreachable during coordinator announce, dropping it: "
                            + e.getMessage());
                    neighbors.remove(neighbor);
                }
            });
        }
    }

    // ============================================================
    //                    JOB / TERM BOOKKEEPING
    // ============================================================

    // Call this when this worker, while acting as coordinator, has assigned `count`
    // jobs to OTHER workers. Updates JAC first, then (if the term hit its limit of 5)
    // steps down and starts a new election, so the election always carries the
    // up-to-date JAC.
    public synchronized void recordJobsAssigned(int count) {
        jac += count;
        jobsThisTerm += count;
        System.out.println("[Worker " + id + "] Assigned " + count + " job(s) (JAC=" + jac
                + ", jobsThisTerm=" + jobsThisTerm + ")");

        if (jobsThisTerm >= 5) {
            System.out.println("[Worker " + id + "] Term limit reached (5 jobs). Stepping down, starting new election.");
            isCoordinator = false;
            coordinatorId = -1;
            coordinatorStub = null;
            jobsThisTerm = 0;
            startElection();
        }
    }

    public void recordJobAssigned() {
        recordJobsAssigned(1);
    }

    // ============================================================
    //                    NETWORK / TOPOLOGY
    // ============================================================

    @Override
    public void addNeighbor(WorkerInterface neighbor) {
        if (neighbor != null && !neighbors.contains(neighbor)) {
            neighbors.add(neighbor);
        }
    }

    // ============================================================
    //                    FAILURE DETECTION
    // ============================================================

    // Periodically pings the known coordinator. If the ping fails, assumes
    // the coordinator is dead and starts a new election. This is what makes
    // "kill the leader process and watch re-election happen" work.
    private void startHeartbeatMonitor() {
        scheduler.scheduleAtFixedRate(() -> {
            WorkerInterface coord = coordinatorStub;
            int knownCoordId = coordinatorId;

            if (coord == null || knownCoordId == -1) {
                return; // no coordinator known yet (election may be in progress)
            }
            if (knownCoordId == this.id) {
                return; // I am the coordinator; nothing to check
            }

            try {
                coord.getId(); // cheap remote call used purely as a liveness probe
            } catch (RemoteException e) {
                System.out.println("[Worker " + id + "] Coordinator (Worker " + knownCoordId
                        + ") appears dead. Starting a new election.");
                coordinatorId = -1;
                coordinatorStub = null;
                startElection();
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // Main method: starts the worker process, registers with bootstrap, and
    // wires up a minimal random neighbor connection for testing the election
    // path end-to-end. (If a teammate owns "join the unstructured network"
    // logic elsewhere, coordinate with them — this is a minimal stand-in.)
    public static void main(String[] args) throws Exception {
        int id = (args.length > 0) ? Integer.parseInt(args[0]) : 1;

        WorkerNode worker = new WorkerNode(id);

        Registry registry = LocateRegistry.getRegistry("localhost", 1099);
        BootstrapService bs = (BootstrapService) registry.lookup("BootstrapService");
        worker.setBootstrap(bs); // needed for job distribution + coordinator announcements

        // Grab whoever is already active BEFORE registering ourselves, so we
        // don't accidentally pick ourselves as our own neighbor.
        List<WorkerInterface> existing = bs.getWorkers();

        bs.registerWorker(worker);
        System.out.println("Worker " + id + " registered with bootstrap.");

        // Best-effort cleanup on a clean exit (Ctrl+C). This is the cheap,
        // instant half of dead-worker cleanup; Bootstrap's getWorkers() also
        // prunes defensively for the hard-crash/kill -9 case this can't catch.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bs.unregisterWorker(worker);
                System.out.println("Worker " + id + " unregistered from bootstrap (shutdown).");
            } catch (RemoteException e) {
                System.out.println("Worker " + id + " could not unregister on shutdown: " + e.getMessage());
            }
        }));

        // Ask directly, in case an election already concluded before we
        // joined and its COORDINATOR flood has already finished propagating.
        worker.adoptCoordinatorFromBootstrap();

        if (!existing.isEmpty()) {
            // Connect to up to 2 existing peers rather than just 1 (see note
            // below on why 1 is fragile). Bootstrap never removes workers
            // that have died, so `existing` can contain stale/dead entries —
            // try candidates in random order and skip any that are actually
            // unreachable, rather than crashing the whole process on one
            // dead stub.
            List<WorkerInterface> shuffled = new ArrayList<>(existing);
            Collections.shuffle(shuffled);

            int connected = 0;
            for (WorkerInterface peer : shuffled) {
                if (connected >= 2) break;
                try {
                    peer.addNeighbor(worker); // also doubles as a liveness check on peer
                    worker.addNeighbor(peer);
                    System.out.println("Worker " + id + " connected to neighbor Worker " + peer.getId());
                    connected++;
                } catch (RemoteException e) {
                    System.out.println("Worker " + id + " skipped a dead/unreachable candidate neighbor: " + e.getMessage());
                }
            }

            // A single connection builds a fragile star topology — if that
            // one peer later dies, everyone connected only to it becomes
            // totally isolated and the network partitions (each island then
            // "correctly" elects its own coordinator, since it has no way of
            // knowing the others exist). Two live connections make that far
            // less likely, though not impossible for a true bridge node.
            if (connected == 0) {
                System.out.println("Worker " + id + " could not reach ANY existing worker to connect to. "
                        + "It is isolated until another worker joins and links to it, or someone adds a neighbor manually.");
            }
        } else {
            System.out.println("Worker " + id + " is first in the network (no neighbors yet).");
            // First worker with no coordinator yet: kick off an election so
            // the system converges on a coordinator as soon as more workers join.
            worker.startElection();
        }
    }
}