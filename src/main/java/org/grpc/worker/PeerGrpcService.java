package org.grpc.worker;

import ds.grpc.Ack;
import ds.grpc.CoordinatorAnnouncement;
import ds.grpc.ElectionReply;
import ds.grpc.ElectionRequest;
import ds.grpc.NodeInfo;
import ds.grpc.PeerServiceGrpc;
import ds.grpc.PingReply;
import ds.grpc.PingRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/** Thin gRPC wrapper around {@link WorkerNode}: translates requests and replies, nothing more. */
public class PeerGrpcService extends PeerServiceGrpc.PeerServiceImplBase {

    private final WorkerNode worker;

    public PeerGrpcService(WorkerNode worker) {
        this.worker = worker;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingReply> out) {
        out.onNext(worker.pingReply());
        out.onCompleted();
    }

    @Override
    public void addNeighbor(NodeInfo request, StreamObserver<Ack> out) {
        try {
            boolean ok = worker.acceptNeighbor(request);
            out.onNext(Ack.newBuilder().setOk(ok).setMessage(ok ? "linked" : "refused").build());
            out.onCompleted();
        } catch (RuntimeException e) {
            fail(out, e);
        }
    }

    @Override
    public void election(ElectionRequest request, StreamObserver<ElectionReply> out) {
        try {
            out.onNext(worker.handleElection(request));
            out.onCompleted();
        } catch (RuntimeException e) {
            fail(out, e);
        }
    }

    @Override
    public void announceCoordinator(CoordinatorAnnouncement request, StreamObserver<Ack> out) {
        try {
            boolean accepted = worker.handleAnnouncement(
                    request.getAnnouncementId(), request.getCoordinator(), request.getSenderId());
            out.onNext(Ack.newBuilder().setOk(accepted)
                    .setMessage(accepted ? "accepted" : "ignored (stale or duplicate)").build());
            out.onCompleted();
        } catch (RuntimeException e) {
            fail(out, e);
        }
    }

    private static void fail(StreamObserver<?> out, RuntimeException e) {
        if (e instanceof StatusRuntimeException) {
            out.onError(e);
        } else {
            out.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
        }
    }
}
