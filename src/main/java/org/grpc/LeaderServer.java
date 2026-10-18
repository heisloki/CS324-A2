package org.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ds.grpc.JobServiceGrpc;
import ds.grpc.JobRequest;
import ds.grpc.JobResponse;
import ds.grpc.JobStatusRequest;
import ds.grpc.JobProgress;

public class LeaderServer {

    private final JobServiceGrpc.JobServiceBlockingStub blockingStub;
    private final JobServiceGrpc.JobServiceStub asyncStub;

    public LeaderServer(String workerHost, int workerPort) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(workerHost, workerPort)
                .usePlaintext()
                .build();

        blockingStub = JobServiceGrpc.newBlockingStub(channel);
        asyncStub = JobServiceGrpc.newStub(channel);
    }

    public void submitJobToWorker() {
        JobRequest request = JobRequest.newBuilder()
                .setJobType("MAX")
                .addNumbers(10)
                .addNumbers(20)
                .addNumbers(5)
                .build();

        JobResponse response = blockingStub.submitJob(request);
        System.out.println("Leader received response: " + response.getStatus());
    }

    public void streamProgressFromWorker(String jobId) {
        JobStatusRequest statusRequest = JobStatusRequest.newBuilder()
                .setJobId(jobId)
                .build();

        asyncStub.streamProgress(statusRequest, new io.grpc.stub.StreamObserver<JobProgress>() {
            @Override
            public void onNext(JobProgress progress) {
                System.out.println("Progress: " + progress.getPercentComplete() +
                        "% from worker " + progress.getWorkerId());
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Error streaming progress: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Progress stream completed.");
            }
        });
    }

    public static void main(String[] args) {
        LeaderServer leader = new LeaderServer("localhost", 50051);
        leader.submitJobToWorker();
        leader.streamProgressFromWorker("job-123");
    }
}
