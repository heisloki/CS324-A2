package org.grpc.bootstrap;

import ds.grpc.Ack;
import ds.grpc.BootstrapServiceGrpc;
import ds.grpc.CoordinatorInfo;
import ds.grpc.Empty;
import ds.grpc.FailureReport;
import ds.grpc.RegisterReply;
import ds.grpc.RegisterRequest;
import ds.grpc.UnregisterRequest;
import ds.grpc.WorkerList;
import io.grpc.stub.StreamObserver;

/** Thin gRPC wrapper around {@link WorkerRegistry}: translates requests and replies, nothing more. */
public class BootstrapGrpcService extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private final WorkerRegistry registry;

    public BootstrapGrpcService(WorkerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterReply> out) {
        WorkerRegistry.RegisterResult r =
                registry.register(request.getHost(), request.getPort(), request.getRequestedId());

        RegisterReply.Builder reply = RegisterReply.newBuilder()
                .setAccepted(r.accepted())
                .setMessage(r.message());
        if (r.accepted()) {
            reply.setSelf(r.self()).addAllPeers(r.peers()).setCoordinator(registry.getCoordinator());
            System.out.println("Bootstrap: Worker " + r.self().getId() + " registered at "
                    + r.self().getHost() + ":" + r.self().getPort() + ", linked to "
                    + r.peers().stream().map(p -> "Worker " + p.getId()).toList());
        } else {
            System.out.println("Bootstrap: registration refused - " + r.message());
        }
        out.onNext(reply.build());
        out.onCompleted();
    }

    @Override
    public void unregister(UnregisterRequest request, StreamObserver<Ack> out) {
        registry.unregister(request.getId());
        reply(out, true, "unregistered");
    }

    @Override
    public void getWorkers(Empty request, StreamObserver<WorkerList> out) {
        out.onNext(WorkerList.newBuilder().addAllWorkers(registry.getWorkers()).build());
        out.onCompleted();
    }

    @Override
    public void reportFailure(FailureReport request, StreamObserver<Ack> out) {
        WorkerRegistry.Outcome o = registry.reportFailure(
                request.getReporterId(), request.getSuspectId(), request.getReason());
        reply(out, o.ok(), o.message());
    }

    @Override
    public void getCoordinator(Empty request, StreamObserver<CoordinatorInfo> out) {
        out.onNext(registry.getCoordinator());
        out.onCompleted();
    }

    @Override
    public void setCoordinator(CoordinatorInfo request, StreamObserver<Ack> out) {
        WorkerRegistry.Outcome o = registry.setCoordinator(request);
        reply(out, o.ok(), o.message());
    }

    private static void reply(StreamObserver<Ack> out, boolean ok, String message) {
        out.onNext(Ack.newBuilder().setOk(ok).setMessage(message).build());
        out.onCompleted();
    }
}
