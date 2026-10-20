package org.grpc.worker;

import ds.grpc.Ack;
import ds.grpc.BootstrapServiceGrpc;
import ds.grpc.Candidate;
import ds.grpc.CoordinatorAnnouncement;
import ds.grpc.CoordinatorInfo;
import ds.grpc.ElectionReply;
import ds.grpc.ElectionRequest;
import ds.grpc.Empty;
import ds.grpc.FailureReport;
import ds.grpc.NodeInfo;
import ds.grpc.PingReply;
import ds.grpc.PingRequest;
import ds.grpc.RegisterReply;
import ds.grpc.RegisterRequest;
import ds.grpc.UnregisterRequest;
import ds.grpc.WorkerList;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.grpc.bootstrap.BootstrapServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A worker process (step 1 of 5: network + election + coordinator tracking).
 *
 * What this class does today:
 *   - starts its own gRPC server and registers with the Bootstrap Node
 *   - links to a few other workers (the unstructured network)
 *   - runs the ECHO-WAVE election (lowest JAC wins, highest id breaks ties)
 *   - tracks who the coordinator is, using terms to ignore stale coordinators
 *   - detects a dead coordinator (heartbeats) and dead neighbours, and repairs the network
 *
 * What is added in later steps (see the two hook methods near the bottom):
 *   JobService + task table (coordinator side), TaskService (worker side), ReplicaService.
 *
 * Usage: WorkerNode <port> [id] [myHost] [bootstrapHost] [bootstrapPort]
 *   id = 0 or omitted -> Bootstrap assigns one.  myHost must be reachable by the OTHER machines.
 */
public class WorkerNode {

    /** Required by the assignment spec: a marker field on every worker node. */
    public static final String leaderman = "cs324";

    // ------------------------------------------------------------------ tunables
    static final long HEARTBEAT_INTERVAL_MS = 1000;    // how often we ping the coordinator
    static final long PING_DEADLINE_MS = 500;
    static final int MAX_MISSED_HEARTBEATS = 2;        // 2 misses in a row = coordinator declared dead
    static final long ELECTION_BUDGET_MS = 8000;       // total time the initiator gives the whole wave
    static final long NO_COORDINATOR_GRACE_MS = 3000;  // how long we wait with no coordinator before electing
    static final long MAINTENANCE_INTERVAL_MS = 2000;  // neighbour checks / re-linking
    static final int TARGET_NEIGHBORS = 2;
    static final long ANNOUNCE_DEADLINE_MS = 2000;
    /** A2 step 2: gRPC's default 4 MB message limit would reject a big CSV job. */
    public static final int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    private static final CoordinatorInfo NO_COORDINATOR =
            CoordinatorInfo.newBuilder().setPresent(false).build();

    private record WaveResult(Candidate best, int visited) { }

    // ------------------------------------------------------------------ identity and plumbing
    private final String host;
    private final int port;
    private final int requestedId;

    private final ManagedChannel bootstrapChannel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private final PeerConnections connections = new PeerConnections();
    private final List<BindableService> services = new ArrayList<>();
    private Server server;
    private volatile NodeInfo self; // null until Bootstrap confirms our id

    private final ExecutorService ioPool = Executors.newCachedThreadPool(daemon("worker-io"));
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, daemon("worker-timer"));
    private final AtomicBoolean shuttingDown = new AtomicBoolean();

    // ------------------------------------------------------------------ network + coordinator state
    private final Map<Integer, NodeInfo> neighbors = new ConcurrentHashMap<>();
    private final Set<String> seenElections = ConcurrentHashMap.newKeySet();
    private final Set<String> seenAnnouncements = ConcurrentHashMap.newKeySet();

    private volatile int jac = 0; // Job Allocation Counter (incremented by the coordinator code in a later step)
    private final AtomicLong highestTermSeen = new AtomicLong(0);
    private final Object coordinatorLock = new Object();
    private volatile CoordinatorInfo coordinator = NO_COORDINATOR;
    private volatile long noCoordinatorSince = System.currentTimeMillis();
    private final AtomicBoolean electionRunning = new AtomicBoolean();
    private final AtomicInteger missedHeartbeats = new AtomicInteger();

    public WorkerNode(String host, int port, int requestedId, String bootstrapHost, int bootstrapPort) {
        this.host = host;
        this.port = port;
        this.requestedId = requestedId;
        this.bootstrapChannel = ManagedChannelBuilder.forAddress(bootstrapHost, bootstrapPort).usePlaintext().build();
        this.bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
        this.services.add(new PeerGrpcService(this));
    }

    /** Later steps register their services here (call before {@link #start()}). */
    public void addService(BindableService service) {
        services.add(service);
    }

    // ------------------------------------------------------------------ lifecycle

    public void start() throws IOException {
        // The server must be listening BEFORE we register: Bootstrap pings us as soon as we are listed.
        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        builder.maxInboundMessageSize(MAX_MESSAGE_BYTES);
        for (BindableService s : services) {
            builder.addService(s);
        }
        server = builder.build().start();

        try {
            registerWithBootstrap();
        } catch (StatusRuntimeException e) {
            shutdown();
            throw new IllegalStateException("Could not reach the Bootstrap Node: " + e.getStatus(), e);
        }

        scheduler.scheduleWithFixedDelay(() -> guarded("heartbeat", this::heartbeatTick),
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> guarded("maintenance", this::maintenanceTick),
                MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        try {
            if (self != null) {
                bootstrap.withDeadlineAfter(2, TimeUnit.SECONDS)
                        .unregister(UnregisterRequest.newBuilder().setId(self.getId()).build());
            }
        } catch (RuntimeException e) {
            // Bootstrap's health checks will notice us anyway.
        }
        scheduler.shutdownNow();
        ioPool.shutdownNow();
        if (server != null) {
            server.shutdown();
        }
        connections.shutdown();
        bootstrapChannel.shutdownNow();
    }

    private void registerWithBootstrap() {
        RegisterReply reply = bootstrap.withDeadlineAfter(5, TimeUnit.SECONDS).register(
                RegisterRequest.newBuilder().setHost(host).setPort(port).setRequestedId(requestedId).build());
        if (!reply.getAccepted()) {
            shutdown();
            throw new IllegalStateException("Bootstrap refused registration: " + reply.getMessage());
        }
        self = reply.getSelf();
        log("registered with Bootstrap as Worker " + self.getId() + " at " + host + ":" + port
                + " (leaderman=" + leaderman + ")");

        for (NodeInfo peer : reply.getPeersList()) {
            if (linkTo(peer)) {
                log("linked to Worker " + peer.getId());
            }
        }

        CoordinatorInfo known = reply.getCoordinator();
        if (known.getPresent() && isAlive(known.getNode())) {
            acceptCoordinator(known); // a late joiner learns the coordinator without waiting for a flood
        } else if (reply.getPeersCount() == 0) {
            log("first worker in the network - electing myself");
            ioPool.submit(() -> startElection("first worker"));
        }
    }

    // ------------------------------------------------------------------ election (echo wave)

    /**
     * Starts an election with this worker as the initiator.
     *
     * The ELECTION message travels outward like a wave; every worker handles a given election_id exactly
     * once and answers its parent with the best candidate found in its part of the network. When the
     * replies come back, the initiator knows the winner of the WHOLE connected network. It then
     * announces the winner (the winner itself is told by the same announcement).
     */
    public void startElection(String reason) {
        if (self == null || !electionRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            long term = highestTermSeen.incrementAndGet();
            String electionId = selfId() + "-" + UUID.randomUUID();
            seenElections.add(electionId);
            log("starting election " + electionId + " for term " + term + " (" + reason + ", my JAC=" + jac + ")");

            WaveResult wave = runWave(electionId, selfId(), term, -1, ELECTION_BUDGET_MS);
            Candidate winner = wave.best();
            log("election done: considered " + wave.visited() + " worker(s), winner is Worker "
                    + winner.getNode().getId() + " (JAC=" + winner.getJac() + ")");

            CoordinatorInfo info = CoordinatorInfo.newBuilder()
                    .setPresent(true).setNode(winner.getNode()).setTerm(term).setJac(winner.getJac()).build();
            handleAnnouncement("coord-" + selfId() + "-" + UUID.randomUUID(), info, selfId());
        } finally {
            electionRunning.set(false);
        }
    }

    /** Called (via PeerService.Election) when a neighbour forwards an election wave to us. */
    ElectionReply handleElection(ElectionRequest req) {
        requireRegistered();
        if (!seenElections.add(req.getElectionId())) {
            return ElectionReply.newBuilder().setAlreadySeen(true).build(); // already handled: adds nothing
        }
        noteTerm(req.getTerm());
        WaveResult wave = runWave(req.getElectionId(), req.getInitiatorId(), req.getTerm(),
                req.getSenderId(), budgetFromContext());
        return ElectionReply.newBuilder()
                .setAlreadySeen(false).setBest(wave.best()).setNodesVisited(wave.visited()).build();
    }

    /** Forward the wave to every neighbour except the parent, in parallel, then merge the answers. */
    private WaveResult runWave(String electionId, int initiatorId, long term, int parentId, long budgetMs) {
        Candidate best = myCandidate();
        int visited = 1;

        ElectionRequest forward = ElectionRequest.newBuilder()
                .setElectionId(electionId).setInitiatorId(initiatorId).setTerm(term).setSenderId(selfId()).build();

        List<Future<ElectionReply>> pending = new ArrayList<>();
        for (NodeInfo n : neighbors.values()) {
            if (n.getId() == parentId) {
                continue;
            }
            pending.add(ioPool.submit(() -> askNeighbor(n, forward, budgetMs)));
        }
        for (Future<ElectionReply> f : pending) {
            ElectionReply r;
            try {
                r = f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                continue;
            }
            if (r == null || r.getAlreadySeen()) {
                continue;
            }
            visited += r.getNodesVisited();
            if (betterThan(r.getBest(), best)) {
                best = r.getBest();
            }
        }
        return new WaveResult(best, visited);
    }

    private ElectionReply askNeighbor(NodeInfo n, ElectionRequest req, long budgetMs) {
        try {
            return connections.peer(n).withDeadlineAfter(budgetMs, TimeUnit.MILLISECONDS).election(req);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                handleNeighborDown(n, "no answer during election (" + e.getStatus().getCode() + ")");
            } else {
                log("Worker " + n.getId() + " was too slow to answer the election (" + e.getStatus().getCode() + ")");
            }
            return null;
        }
    }

    /**
     * Each hop must finish BEFORE its caller's deadline, otherwise a slow node deep in the network would
     * make every ancestor give up at once. So a node gives its children the time it has left, minus a margin.
     */
    private long budgetFromContext() {
        Deadline deadline = Context.current().getDeadline();
        if (deadline == null) {
            return ELECTION_BUDGET_MS;
        }
        return Math.max(200, deadline.timeRemaining(TimeUnit.MILLISECONDS) - 300);
    }

    private Candidate myCandidate() {
        return Candidate.newBuilder().setNode(self).setJac(jac).build();
    }

    /** Winner rule from the assignment: LOWEST JAC wins; on a tie the HIGHEST id wins. */
    private static boolean betterThan(int jacA, int idA, int jacB, int idB) {
        return jacA != jacB ? jacA < jacB : idA > idB;
    }

    private static boolean betterThan(Candidate a, Candidate b) {
        return betterThan(a.getJac(), a.getNode().getId(), b.getJac(), b.getNode().getId());
    }

    // ------------------------------------------------------------------ coordinator announcement

    /**
     * Handles a COORDINATOR announcement (flooded to everyone). Returns true if it was new and accepted.
     * Stale terms are refused, so a slow or zombie coordinator can never override a newer one.
     */
    boolean handleAnnouncement(String announcementId, CoordinatorInfo info, int senderId) {
        requireRegistered();
        if (!seenAnnouncements.add(announcementId)) {
            return false; // each announcement is processed once
        }
        if (!acceptCoordinator(info)) {
            return false;
        }
        forwardAnnouncement(announcementId, info, senderId);
        if (info.getNode().getId() == selfId()) {
            ioPool.submit(() -> becomeCoordinator(info)); // the announcement tells the winner that it won
        }
        return true;
    }

    private void forwardAnnouncement(String announcementId, CoordinatorInfo info, int senderId) {
        CoordinatorAnnouncement msg = CoordinatorAnnouncement.newBuilder()
                .setAnnouncementId(announcementId).setCoordinator(info).setSenderId(selfId()).build();
        for (NodeInfo n : neighbors.values()) {
            if (n.getId() == senderId) {
                continue;
            }
            ioPool.submit(() -> {
                try {
                    connections.peer(n).withDeadlineAfter(ANNOUNCE_DEADLINE_MS, TimeUnit.MILLISECONDS)
                            .announceCoordinator(msg);
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                        handleNeighborDown(n, "no answer to coordinator announcement");
                    }
                }
            });
        }
    }

    /** Decides whether to adopt {@code info}. Order: higher term wins; same term -> better (JAC, id) wins. */
    private boolean acceptCoordinator(CoordinatorInfo info) {
        boolean lostRole = false;
        synchronized (coordinatorLock) {
            noteTerm(info.getTerm());
            CoordinatorInfo current = coordinator;
            if (current.getPresent()) {
                if (info.getTerm() < current.getTerm()) {
                    return false; // stale
                }
                if (info.getTerm() == current.getTerm()) {
                    if (info.getNode().getId() == current.getNode().getId()) {
                        return false; // already known: not new, so nothing to forward and no second "become coordinator"
                    }
                    if (!betterThan(info.getJac(), info.getNode().getId(),
                            current.getJac(), current.getNode().getId())) {
                        return false; // two elections tied on term: keep the better candidate
                    }
                }
                lostRole = current.getNode().getId() == selfId() && info.getNode().getId() != selfId();
            }
            coordinator = info;
            missedHeartbeats.set(0);
        }
        log("coordinator is now Worker " + info.getNode().getId()
                + " (term " + info.getTerm() + ", JAC=" + info.getJac() + ")");
        if (lostRole) {
            onLostCoordinatorRole();
        }
        return true;
    }

    /** Runs on the worker that was named coordinator. */
    private void becomeCoordinator(CoordinatorInfo info) {
        log("=== I AM THE COORDINATOR (term " + info.getTerm() + ", JAC=" + info.getJac()
                + ", leaderman=" + leaderman + ") ===");
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Ack ack = bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS).setCoordinator(info);
                if (!ack.getOk()) {
                    log("Bootstrap did not record me as coordinator: " + ack.getMessage());
                }
                break;
            } catch (StatusRuntimeException e) {
                log("could not tell Bootstrap I am coordinator (attempt " + attempt + "): " + e.getStatus().getCode());
            }
        }
        onBecameCoordinator(info.getTerm());
    }

    // ------------------------------------------------------------------ failure detection

    /** Every second: ping the coordinator we follow. Two misses in a row -> it is dead, elect a new one. */
    private void heartbeatTick() {
        if (self == null || shuttingDown.get()) {
            return;
        }
        CoordinatorInfo c = coordinator;
        if (!c.getPresent()) {
            noCoordinatorTick();
            return;
        }
        if (isCoordinator()) {
            return; // nobody to watch: I am the one being watched
        }
        try {
            PingReply r = connections.peer(c.getNode())
                    .withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .ping(PingRequest.newBuilder().setSenderId(selfId()).build());
            missedHeartbeats.set(0);
            if (r.getTerm() > c.getTerm()) {
                refreshCoordinatorFromBootstrap(); // the one I follow knows of a newer coordinator
            }
        } catch (StatusRuntimeException e) {
            connections.forget(c.getNode());
            int missed = missedHeartbeats.incrementAndGet();
            log("missed heartbeat " + missed + "/" + MAX_MISSED_HEARTBEATS + " from coordinator Worker "
                    + c.getNode().getId());
            if (missed >= MAX_MISSED_HEARTBEATS) {
                coordinatorDown(c);
            }
        }
    }

    private void coordinatorDown(CoordinatorInfo dead) {
        synchronized (coordinatorLock) {
            if (coordinator != dead) {
                return; // someone already replaced it
            }
            coordinator = NO_COORDINATOR;
            noCoordinatorSince = System.currentTimeMillis();
        }
        missedHeartbeats.set(0);
        log("coordinator Worker " + dead.getNode().getId() + " is DEAD - starting an election");
        reportFailureAsync(dead.getNode().getId(), "coordinator missed heartbeats");
        electionAfterJitter("coordinator failed");
    }

    /** A small random pause so that workers noticing the failure together do not all start elections at once. */
    private void electionAfterJitter(String reason) {
        ioPool.submit(() -> {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(0, 400));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!coordinator.getPresent()) { // another worker's election may already have finished
                startElection(reason);
            }
        });
    }

    /** No coordinator known: first ask Bootstrap, and only if it has none either, hold an election. */
    private void noCoordinatorTick() {
        if (electionRunning.get() || System.currentTimeMillis() - noCoordinatorSince < NO_COORDINATOR_GRACE_MS) {
            return;
        }
        refreshCoordinatorFromBootstrap();
        if (!coordinator.getPresent()) {
            noCoordinatorSince = System.currentTimeMillis();
            startElection("no coordinator known");
        }
    }

    private void refreshCoordinatorFromBootstrap() {
        try {
            CoordinatorInfo b = bootstrap.withDeadlineAfter(2, TimeUnit.SECONDS).getCoordinator(Empty.getDefaultInstance());
            if (b.getPresent() && b.getTerm() > coordinator.getTerm() && isAlive(b.getNode())) {
                acceptCoordinator(b);
            }
        } catch (StatusRuntimeException e) {
            log("Bootstrap unreachable (" + e.getStatus().getCode() + ") - carrying on without it");
        }
    }

    /** Every 2 s: ping neighbours, drop dead ones, and top up to TARGET_NEIGHBORS so the network stays connected. */
    private void maintenanceTick() {
        if (self == null || shuttingDown.get()) {
            return;
        }
        for (NodeInfo n : new ArrayList<>(neighbors.values())) {
            try {
                connections.peer(n).withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                        .ping(PingRequest.newBuilder().setSenderId(selfId()).build());
            } catch (StatusRuntimeException e) {
                handleNeighborDown(n, "neighbour check failed (" + e.getStatus().getCode() + ")");
            }
        }
        if (neighbors.size() < TARGET_NEIGHBORS) {
            replenishNeighbors();
        }
    }

    private void replenishNeighbors() {
        try {
            WorkerList all = bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS).getWorkers(Empty.getDefaultInstance());
            List<NodeInfo> candidates = new ArrayList<>(all.getWorkersList());
            candidates.removeIf(n -> n.getId() == selfId() || neighbors.containsKey(n.getId()));
            Collections.shuffle(candidates);
            for (NodeInfo c : candidates) {
                if (neighbors.size() >= TARGET_NEIGHBORS) {
                    break;
                }
                if (linkTo(c)) {
                    log("linked to replacement neighbour Worker " + c.getId());
                }
            }
        } catch (StatusRuntimeException e) {
            log("could not fetch the worker list from Bootstrap (" + e.getStatus().getCode() + ")");
        }
    }

    private void handleNeighborDown(NodeInfo n, String why) {
        if (neighbors.remove(n.getId(), n)) {
            connections.forget(n);
            log("neighbour Worker " + n.getId() + " dropped: " + why);
            reportFailureAsync(n.getId(), why);
        }
    }

    /** Bootstrap pings the suspect itself before removing anything, so a false alarm is harmless. */
    private void reportFailureAsync(int suspectId, String reason) {
        ioPool.submit(() -> {
            try {
                bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS).reportFailure(FailureReport.newBuilder()
                        .setReporterId(selfId()).setSuspectId(suspectId).setReason(reason).build());
            } catch (RuntimeException e) {
                // Bootstrap's own health checks will catch it.
            }
        });
    }

    // ------------------------------------------------------------------ linking and small helpers

    private boolean linkTo(NodeInfo peer) {
        try {
            Ack ack = connections.peer(peer).withDeadlineAfter(2, TimeUnit.SECONDS).addNeighbor(self);
            if (!ack.getOk()) {
                return false;
            }
            neighbors.put(peer.getId(), peer);
            return true;
        } catch (StatusRuntimeException e) {
            connections.forget(peer);
            return false;
        }
    }

    /** Called (via PeerService.AddNeighbor) when another worker links to us. */
    boolean acceptNeighbor(NodeInfo node) {
        requireRegistered();
        if (node.getId() <= 0 || node.getId() == selfId()) {
            return false;
        }
        neighbors.put(node.getId(), node);
        return true;
    }

    private boolean isAlive(NodeInfo node) {
        try {
            connections.peer(node).withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .ping(PingRequest.newBuilder().setSenderId(selfId()).build());
            return true;
        } catch (StatusRuntimeException e) {
            connections.forget(node);
            return false;
        }
    }

    /** Answers answer pings even while still registering, so Bootstrap's health check never fails us early. */
    PingReply pingReply() {
        CoordinatorInfo c = coordinator;
        return PingReply.newBuilder()
                .setId(selfId()).setJac(jac).setTerm(c.getTerm())
                .setCoordinatorId(c.getPresent() ? c.getNode().getId() : 0)
                .build();
    }

    private void noteTerm(long term) {
        highestTermSeen.accumulateAndGet(term, Math::max);
    }

    private void requireRegistered() {
        if (self == null) {
            throw Status.UNAVAILABLE.withDescription("worker is still starting up").asRuntimeException();
        }
    }

    private int selfId() {
        NodeInfo s = self;
        return s == null ? 0 : s.getId();
    }

    private void guarded(String name, Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            System.out.println("[Worker " + selfId() + "] " + name + " task failed: " + t);
        }
    }

    private static ThreadFactory daemon(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private void log(String message) {
        System.out.println("[Worker " + selfId() + "] " + message);
    }

    // ------------------------------------------------------------------ accessors for later steps

    public NodeInfo getSelf() { return self; }
    public int getJac() { return jac; }

    /** A2 step 2: the coordinator calls this each time it hands a task to ANOTHER worker (that is what JAC counts). */
    public synchronized void incrementJac(int by) { jac += by; }

    /** A2 step 2: the coordinator needs Bootstrap to list live workers and to report failures. */
    public BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapStub() { return bootstrap; }
    public CoordinatorInfo getCoordinator() { return coordinator; }
    public boolean isCoordinator() {
        CoordinatorInfo c = coordinator;
        return c.getPresent() && c.getNode().getId() == selfId();
    }
    public PeerConnections connections() { return connections; }
    public List<NodeInfo> neighborList() { return new ArrayList<>(neighbors.values()); }

    // ------------------------------------------------------------------ hooks (filled in by steps 2-4)

    /** This worker just became coordinator for {@code term}. Step 2/4: fetch replicated state, resume unfinished jobs. */
    protected void onBecameCoordinator(long term) {
        // later steps
    }

    /** A newer coordinator replaced this worker. Step 2: stop accepting jobs. */
    protected void onLostCoordinatorRole() {
        // later steps
    }

    // ------------------------------------------------------------------ main

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: WorkerNode <port> [id] [myHost] [bootstrapHost] [bootstrapPort]");
            return;
        }
        int port = Integer.parseInt(args[0]);
        int id = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        String myHost = args.length > 2 ? args[2] : "localhost";
        String bootstrapHost = args.length > 3 ? args[3] : "localhost";
        int bootstrapPort = args.length > 4 ? Integer.parseInt(args[4]) : BootstrapServer.DEFAULT_PORT;

        WorkerNode worker = new WorkerNode(myHost, port, id, bootstrapHost, bootstrapPort);
        worker.start();
        worker.awaitTermination();
    }
}