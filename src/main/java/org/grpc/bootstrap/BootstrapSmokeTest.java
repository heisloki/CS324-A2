package org.grpc.bootstrap;

import ds.grpc.Ack;
import ds.grpc.BootstrapServiceGrpc;
import ds.grpc.CoordinatorInfo;
import ds.grpc.Empty;
import ds.grpc.FailureReport;
import ds.grpc.NodeInfo;
import ds.grpc.PeerServiceGrpc;
import ds.grpc.PingReply;
import ds.grpc.PingRequest;
import ds.grpc.RegisterReply;
import ds.grpc.RegisterRequest;
import ds.grpc.UnregisterRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Checks a RUNNING Bootstrap Node through its real gRPC interface, using fake workers that only
 * answer Ping.   Usage: start BootstrapServer first, then run BootstrapSmokeTest [bootstrapPort].
 * Exits with a non-zero status if any check fails.
 */
public class BootstrapSmokeTest {

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        int bootstrapPort = args.length > 0 ? Integer.parseInt(args[0]) : BootstrapServer.DEFAULT_PORT;
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", bootstrapPort).usePlaintext().build();
        BootstrapServiceGrpc.BootstrapServiceBlockingStub bs = BootstrapServiceGrpc.newBlockingStub(channel);

        Server w1 = fakeWorker(51001), w2 = fakeWorker(51002), w3 = fakeWorker(51003), w7 = fakeWorker(51007);

        System.out.println("--- registration and unique ids ---");
        RegisterReply a = register(bs, 51001, 0);
        check("first worker is accepted with id 1", a.getAccepted() && a.getSelf().getId() == 1);
        check("first worker has no peers yet", a.getPeersCount() == 0);
        check("no coordinator yet", !a.getCoordinator().getPresent());

        RegisterReply b = register(bs, 51002, 0);
        check("second worker gets id 2 and is linked to worker 1",
                b.getSelf().getId() == 2 && ids(b.getPeersList()).equals(List.of(1)));

        RegisterReply c = register(bs, 51003, 0);
        check("third worker gets id 3 and is linked to two live workers", c.getSelf().getId() == 3 && c.getPeersCount() == 2);

        RegisterReply dup = register(bs, 51009, 2);
        check("requesting an id held by a LIVE worker is rejected", !dup.getAccepted());

        RegisterReply d = register(bs, 51007, 7);
        check("requesting a free id (7) is accepted", d.getAccepted() && d.getSelf().getId() == 7);
        RegisterReply e = register(bs, 51010, 0);
        check("auto-assigned ids skip past manually chosen ones (next is 8)", e.getSelf().getId() == 8);
        bs.unregister(UnregisterRequest.newBuilder().setId(8).build());
        check("workers 1,2,3,7 are listed",
                ids(bs.getWorkers(Empty.getDefaultInstance()).getWorkersList()).equals(List.of(1, 2, 3, 7)));

        System.out.println("--- coordinator tracking and terms ---");
        check("worker 3 becomes coordinator in term 1", setCoord(bs, 3, 1).getOk());
        CoordinatorInfo cur = bs.getCoordinator(Empty.getDefaultInstance());
        check("GetCoordinator reports worker 3, term 1", cur.getPresent() && cur.getNode().getId() == 3 && cur.getTerm() == 1);
        check("a different worker in the SAME term is rejected", !setCoord(bs, 2, 1).getOk());
        check("a higher term replaces the coordinator (worker 1, term 2)", setCoord(bs, 1, 2).getOk());
        check("a stale term is rejected", !setCoord(bs, 3, 1).getOk());

        System.out.println("--- failure detection (waits for the health checks, about 5 s) ---");
        w1.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); // the coordinator dies
        boolean pruned = waitUntil(8000, () ->
                !ids(bs.getWorkers(Empty.getDefaultInstance()).getWorkersList()).contains(1));
        check("the dead coordinator is removed by the health checks", pruned);
        check("clients are NOT handed a dead coordinator", !bs.getCoordinator(Empty.getDefaultInstance()).getPresent());

        w1 = fakeWorker(51001);
        RegisterReply back = register(bs, 51001, 1);
        check("the crashed worker can re-register under its old id", back.getAccepted() && back.getSelf().getId() == 1);
        check("an old, stale term is still refused after the restart", !setCoord(bs, 1, 1).getOk());

        System.out.println("--- ReportFailure and graceful leave ---");
        check("reporting a LIVE worker does not remove it",
                !bs.reportFailure(report(3, 2)).getOk()
                        && ids(bs.getWorkers(Empty.getDefaultInstance()).getWorkersList()).contains(2));
        w2.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        check("reporting a DEAD worker removes it immediately", bs.reportFailure(report(3, 2)).getOk()
                && !ids(bs.getWorkers(Empty.getDefaultInstance()).getWorkersList()).contains(2));
        bs.unregister(UnregisterRequest.newBuilder().setId(3).build());
        check("a worker that leaves gracefully is removed",
                !ids(bs.getWorkers(Empty.getDefaultInstance()).getWorkersList()).contains(3));

        System.out.println("--- restart at the same address ---");
        RegisterReply again = register(bs, 51007, 0);
        long samePort = bs.getWorkers(Empty.getDefaultInstance()).getWorkersList().stream()
                .filter(n -> n.getPort() == 51007).count();
        check("re-registering from the same address replaces the old entry (no duplicate)", again.getAccepted() && samePort == 1);

        for (Server s : List.of(w1, w2, w3, w7)) {
            s.shutdownNow();
        }
        channel.shutdownNow();
        System.out.println(failures == 0 ? "\nALL CHECKS PASSED" : "\n" + failures + " CHECK(S) FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }

    // ------------------------------------------------------------------ helpers

    private static RegisterReply register(BootstrapServiceGrpc.BootstrapServiceBlockingStub bs, int port, int wantedId) {
        return bs.register(RegisterRequest.newBuilder().setHost("localhost").setPort(port).setRequestedId(wantedId).build());
    }

    private static Ack setCoord(BootstrapServiceGrpc.BootstrapServiceBlockingStub bs, int id, long term) {
        NodeInfo node = NodeInfo.newBuilder().setId(id).setHost("localhost").setPort(51000 + id).build();
        return bs.setCoordinator(CoordinatorInfo.newBuilder().setPresent(true).setNode(node).setTerm(term).build());
    }

    private static FailureReport report(int reporter, int suspect) {
        return FailureReport.newBuilder().setReporterId(reporter).setSuspectId(suspect).setReason("smoke test").build();
    }

    private static List<Integer> ids(List<NodeInfo> nodes) {
        return nodes.stream().map(NodeInfo::getId).sorted().toList();
    }

    private static Server fakeWorker(int port) throws Exception {
        return ServerBuilder.forPort(port).addService(new PeerServiceGrpc.PeerServiceImplBase() {
            @Override
            public void ping(PingRequest request, StreamObserver<PingReply> out) {
                out.onNext(PingReply.newBuilder().setId(port).build());
                out.onCompleted();
            }
        }).build().start();
    }

    private static boolean waitUntil(long millis, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(250);
        }
        return condition.getAsBoolean();
    }

    private static void check(String what, boolean ok) {
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what);
        if (!ok) {
            failures++;
        }
    }
}
