package org.grpc.coordinator;

import ds.grpc.JobProgress;
import ds.grpc.JobServiceGrpc;
import ds.grpc.JobState;
import ds.grpc.SubmitJobReply;
import ds.grpc.SubmitJobRequest;
import ds.grpc.WatchJobRequest;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Thin gRPC wrapper around {@link JobEngine}: what clients talk to. Only does real work on the coordinator. */
public class JobGrpcService extends JobServiceGrpc.JobServiceImplBase {

    private static final long POLL_MS = 100;

    private final JobEngine engine;
    private final ExecutorService watchPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "watch-job");
        t.setDaemon(true);
        return t;
    });

    public JobGrpcService(JobEngine engine) {
        this.engine = engine;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobReply> out) {
        out.onNext(engine.submit(request));
        out.onCompleted();
    }

    /** Streams the job's real progress (about 10 updates per second at most) until it ends. */
    @Override
    public void watchJob(WatchJobRequest request, StreamObserver<JobProgress> out) {
        ServerCallStreamObserver<JobProgress> call = (ServerCallStreamObserver<JobProgress>) out;
        watchPool.submit(() -> {
            JobProgress last = null;
            try {
                while (!call.isCancelled()) {
                    JobProgress p = engine.snapshot(request.getJobId());
                    if (p == null) {
                        out.onError(Status.NOT_FOUND
                                .withDescription("this coordinator does not know job " + request.getJobId())
                                .asRuntimeException());
                        return;
                    }
                    if (!p.equals(last)) {
                        out.onNext(p);
                        last = p;
                    }
                    if (p.getState() != JobState.JOB_STATE_RUNNING) {
                        out.onCompleted();
                        return;
                    }
                    Thread.sleep(POLL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                // the client went away; nothing left to do
            }
        });
    }
}
