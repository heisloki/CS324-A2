package org.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ds.grpc.JobServiceGrpc;
import ds.grpc.JobRequest;
import ds.grpc.JobResponse;
import ds.grpc.JobStatusRequest;
import ds.grpc.JobProgress;

public class WorkerServer {
    private Server server;

    public void start() throws Exception {
        server = ServerBuilder.forPort(50051)
                .addService(new JobServiceImpl())
                .build()
                .start();

        System.out.println("Worker gRPC server started on port 50051");
        server.awaitTermination();
    }

    static class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
        @Override
        public void submitJob(JobRequest request, StreamObserver<JobResponse> responseObserver) {
            // Example: handle MAX job
            int result = request.getNumbersList().stream()
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);

            JobResponse response = JobResponse.newBuilder()
                    .setJobId("job-123")
                    .setStatus("Result: " + result)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void streamProgress(JobStatusRequest request, StreamObserver<JobProgress> responseObserver) {
            // Example: simulate progress updates
            for (int i = 0; i <= 100; i += 20) {
                JobProgress progress = JobProgress.newBuilder()
                        .setPercentComplete(i)
                        .setWorkerId("worker-1")
                        .build();
                responseObserver.onNext(progress);

                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            }
            responseObserver.onCompleted();
        }
    }

    public static void main(String[] args) throws Exception {
        new WorkerServer().start();
    }
}
