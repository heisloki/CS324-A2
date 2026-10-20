
package org.grpc.worker;

import ds.grpc.Ack;
import ds.grpc.Empty;
import ds.grpc.ReplicaServiceGrpc;
import ds.grpc.StateSnapshot;
import io.grpc.stub.StreamObserver;

/**
 * gRPC service that allows workers to store and retrieve
 * replicated coordinator state.
 */
public final class ReplicaGrpcService
        extends ReplicaServiceGrpc.ReplicaServiceImplBase {

    private final ReplicaStore store;

    public ReplicaGrpcService(ReplicaStore store) {
        this.store = store;
    }

    /**
     * Receive and store a coordinator state snapshot.
     */
    @Override
    public void replicateState(
            StateSnapshot request,
            StreamObserver<Ack> responseObserver) {

        boolean accepted = store.store(request);

        Ack reply = Ack.newBuilder()
                .setOk(accepted)
                .setMessage(
                        accepted
                                ? "Snapshot stored"
                                : "Snapshot rejected: stale or conflicting state")
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    /**
     * Return the most recently stored coordinator snapshot.
     */
    @Override
    public void fetchState(
            Empty request,
            StreamObserver<StateSnapshot> responseObserver) {

        responseObserver.onNext(store.getSnapshot());
        responseObserver.onCompleted();
    }
}
