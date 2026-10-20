package org.grpc.bootstrap;

import ds.grpc.CoordinatorInfo;
import ds.grpc.NodeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Bootstrap Node's brain: who is registered, who is alive, who is coordinator.
 *
 * Deliberately contains no gRPC server code so it can be tested on its own. The only network
 * action it needs, "is this worker alive?", is injected as a Prober.
 *
 * It does not take part in elections or job processing; it only stores facts, as the spec requires.
 */
public class WorkerRegistry {

    /** Asks a worker "are you alive?" (in production: a PeerService.Ping with a short deadline). */
    public interface Prober {
        boolean isAlive(NodeInfo node);

        /** Called when a worker is removed so the prober can drop any cached connection. */
        default void forget(NodeInfo node) { }
    }

    public record RegisterResult(boolean accepted, String message, NodeInfo self, List<NodeInfo> peers) { }

    public record Outcome(boolean ok, String message) { }

    /** How many random live workers a newly joined worker is linked to. */
    public static final int PEER_LINKS = 2;
    /** A worker is removed after this many consecutive failed health checks (about 4 s at a 2 s interval). */
    public static final int MAX_MISSED_PINGS = 2;

    private static final CoordinatorInfo NO_COORDINATOR =
            CoordinatorInfo.newBuilder().setPresent(false).build();

    private final Prober prober;
    private final Random random = new Random();

    // everything below is guarded by "this"
    private final Map<Integer, NodeInfo> workers = new LinkedHashMap<>();
    private final Map<Integer, Integer> missedPings = new HashMap<>();
    private int nextId = 1;
    private CoordinatorInfo coordinator = NO_COORDINATOR;
    private long highestTerm = 0; // never decreases, even while there is no coordinator

    private final ExecutorService probePool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bootstrap-probe");
        t.setDaemon(true);
        return t;
    });
    private ScheduledExecutorService healthScheduler;

    public WorkerRegistry(Prober prober) {
        this.prober = prober;
    }

    // ------------------------------------------------------------------ registration

    public RegisterResult register(String host, int port, int requestedId) {
        if (host == null || host.isBlank() || port <= 0) {
            return rejected("host and port are required");
        }
        if (requestedId < 0) {
            return rejected("requested_id must be 0 (auto-assign) or a positive number");
        }

        // If the wanted id belongs to a worker at a DIFFERENT address, find out (outside the lock,
        // it is a network call) whether that worker is still alive.
        NodeInfo holder;
        synchronized (this) {
            holder = requestedId > 0 ? workers.get(requestedId) : null;
        }
        boolean holderAlive = false;
        if (holder != null && !sameAddress(holder, host, port)) {
            holderAlive = prober.isAlive(holder);
        }

        NodeInfo self;
        synchronized (this) {
            NodeInfo nowHolder = requestedId > 0 ? workers.get(requestedId) : null;

            if (nowHolder != null && !sameAddress(nowHolder, host, port)
                    && (holderAlive || !nowHolder.equals(holder))) {
                return rejected("ID " + requestedId + " is already used by a live worker at "
                        + nowHolder.getHost() + ":" + nowHolder.getPort());
            }

            // Whatever is still registered at this address is a previous run of this worker: replace it.
            for (NodeInfo old : new ArrayList<>(workers.values())) {
                if (sameAddress(old, host, port)) {
                    removeLocked(old.getId(), "replaced by a new registration at the same address");
                }
            }
            // A dead worker that held the requested id gives it up.
            if (nowHolder != null && workers.containsKey(requestedId)) {
                removeLocked(requestedId, "id taken over (previous holder is dead)");
            }

            int id = requestedId > 0 ? requestedId : allocateIdLocked();
            self = NodeInfo.newBuilder().setId(id).setHost(host).setPort(port).build();
            workers.put(id, self);
            missedPings.put(id, 0);
            if (nextId <= id) {
                nextId = id + 1; // keep auto-assigned ids clear of manually chosen ones
            }
        }

        List<NodeInfo> peers = pickLivePeers(self.getId(), PEER_LINKS);
        return new RegisterResult(true, "registered", self, peers);
    }

    private int allocateIdLocked() {
        while (workers.containsKey(nextId)) {
            nextId++;
        }
        return nextId++;
    }

    /** Random live workers other than {@code selfId}; each candidate is probed so a worker is never linked to a dead one. */
    private List<NodeInfo> pickLivePeers(int selfId, int wanted) {
        List<NodeInfo> candidates;
        synchronized (this) {
            candidates = new ArrayList<>(workers.values());
        }
        candidates.removeIf(n -> n.getId() == selfId);
        Collections.shuffle(candidates, random);

        List<NodeInfo> peers = new ArrayList<>();
        for (NodeInfo candidate : candidates) {
            if (peers.size() >= wanted) {
                break;
            }
            if (prober.isAlive(candidate)) {
                peers.add(candidate);
            }
        }
        return peers;
    }

    public void unregister(int id) {
        synchronized (this) {
            if (workers.containsKey(id)) {
                removeLocked(id, "left gracefully");
            }
        }
    }

    public synchronized List<NodeInfo> getWorkers() {
        return new ArrayList<>(workers.values());
    }

    // ------------------------------------------------------------------ failures

    /** A worker reports that it cannot reach {@code suspectId}. Bootstrap checks for itself before removing anything. */
    public Outcome reportFailure(int reporterId, int suspectId, String reason) {
        NodeInfo suspect;
        synchronized (this) {
            suspect = workers.get(suspectId);
        }
        if (suspect == null) {
            return new Outcome(true, "worker " + suspectId + " is not registered (already removed)");
        }
        if (prober.isAlive(suspect)) {
            return new Outcome(false, "worker " + suspectId + " answered Bootstrap's ping; not removed");
        }
        synchronized (this) {
            if (suspect.equals(workers.get(suspectId))) {
                removeLocked(suspectId, "reported dead by worker " + reporterId + " (" + reason + ") and confirmed");
            }
        }
        return new Outcome(true, "worker " + suspectId + " confirmed dead and removed");
    }

    /** One health-check round: ping every worker in parallel, remove the ones that keep failing. */
    public void runHealthCheck() {
        List<NodeInfo> snapshot = getWorkers();
        List<Future<Boolean>> results = new ArrayList<>();
        for (NodeInfo node : snapshot) {
            Callable<Boolean> probe = () -> prober.isAlive(node);
            results.add(probePool.submit(probe));
        }
        for (int i = 0; i < snapshot.size(); i++) {
            boolean alive;
            try {
                alive = results.get(i).get();
            } catch (Exception e) {
                alive = false;
            }
            recordProbe(snapshot.get(i), alive);
        }
    }

    private synchronized void recordProbe(NodeInfo node, boolean alive) {
        if (!node.equals(workers.get(node.getId()))) {
            return; // it was removed or re-registered while we were pinging
        }
        if (alive) {
            missedPings.put(node.getId(), 0);
            return;
        }
        int missed = missedPings.merge(node.getId(), 1, Integer::sum);
        if (missed >= MAX_MISSED_PINGS) {
            removeLocked(node.getId(), "failed " + missed + " health checks in a row");
        }
    }

    public void startHealthChecks(long intervalMillis) {
        healthScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bootstrap-health");
            t.setDaemon(true);
            return t;
        });
        healthScheduler.scheduleWithFixedDelay(() -> {
            try {
                runHealthCheck();
            } catch (RuntimeException e) {
                System.out.println("Bootstrap: health check round failed: " + e);
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (healthScheduler != null) {
            healthScheduler.shutdownNow();
        }
        probePool.shutdownNow();
    }

    private void removeLocked(int id, String why) {
        NodeInfo removed = workers.remove(id);
        missedPings.remove(id);
        if (removed == null) {
            return;
        }
        System.out.println("Bootstrap: removed Worker " + id + " (" + why + ")");
        prober.forget(removed);
        // Never hand clients the address of a dead coordinator.
        if (coordinator.getPresent() && coordinator.getNode().getId() == id) {
            coordinator = NO_COORDINATOR;
            System.out.println("Bootstrap: the coordinator is gone - no coordinator until a new one is elected");
        }
    }

    // ------------------------------------------------------------------ coordinator

    public synchronized CoordinatorInfo getCoordinator() {
        return coordinator;
    }

    /** Called by the winner of an election. Older terms are refused, so a slow or zombie coordinator cannot overwrite a newer one. */
    public synchronized Outcome setCoordinator(CoordinatorInfo candidate) {
        if (!candidate.getPresent() || candidate.getNode().getId() <= 0) {
            return new Outcome(false, "a coordinator must be present and have a positive id");
        }
        if (candidate.getTerm() < highestTerm) {
            return new Outcome(false, "stale term " + candidate.getTerm() + " (current is " + highestTerm + ")");
        }
        if (candidate.getTerm() == highestTerm && coordinator.getPresent()
                && coordinator.getNode().getId() != candidate.getNode().getId()) {
            return new Outcome(false, "term " + highestTerm + " already has coordinator Worker "
                    + coordinator.getNode().getId());
        }
        // The winner is evidently alive; make sure it is in the registry even if it was pruned by mistake.
        NodeInfo node = candidate.getNode();
        if (!workers.containsKey(node.getId())) {
            workers.put(node.getId(), node);
            missedPings.put(node.getId(), 0);
        }
        coordinator = candidate;
        highestTerm = candidate.getTerm();
        System.out.println("Bootstrap: coordinator is now Worker " + node.getId()
                + " (term " + candidate.getTerm() + ", JAC=" + candidate.getJac() + ")");
        return new Outcome(true, "coordinator recorded");
    }

    // ------------------------------------------------------------------ helpers

    private static boolean sameAddress(NodeInfo n, String host, int port) {
        return n.getHost().equals(host) && n.getPort() == port;
    }

    private static RegisterResult rejected(String why) {
        return new RegisterResult(false, why, NodeInfo.getDefaultInstance(), List.of());
    }
}
