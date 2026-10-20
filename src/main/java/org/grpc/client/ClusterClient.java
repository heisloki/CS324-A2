package org.grpc.client;

import ds.grpc.BootstrapServiceGrpc;
import ds.grpc.CoordinatorInfo;
import ds.grpc.Empty;
import ds.grpc.JobProgress;
import ds.grpc.JobServiceGrpc;
import ds.grpc.JobState;
import ds.grpc.SubmitJobReply;
import ds.grpc.SubmitJobRequest;
import ds.grpc.WatchJobRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Everything a client needs to run a job on the cluster: find the coordinator through Bootstrap, submit,
 * and follow the real progress stream. It copes with the coordinator changing (a term ending, or a crash)
 * by asking Bootstrap again and re-attaching with the SAME job id - which is why the id is chosen by the
 * client: re-sending a job never creates a duplicate.
 *
 * The GUI will use this class too (it only needs the two Listener callbacks).
 */
public class ClusterClient implements AutoCloseable {

    public interface Listener {
        /** A new progress snapshot (percent, per-worker tasks, ...). */
        void onProgress(JobProgress progress);

        /** Something the user should know: "no coordinator yet", "coordinator lost, reconnecting", ... */
        void onNote(String note);
    }

    private static final int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    private final ManagedChannel bootstrapChannel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private ManagedChannel coordinatorChannel;
    private String coordinatorKey = "";

    public ClusterClient(String bootstrapHost, int bootstrapPort) {
        bootstrapChannel = ManagedChannelBuilder.forAddress(bootstrapHost, bootstrapPort)
                .usePlaintext().maxInboundMessageSize(MAX_MESSAGE_BYTES).build();
        bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
    }

    /**
     * Submits the job and blocks until it finishes. Returns the final progress (COMPLETED or FAILED).
     * Safe to call from several threads at once: that is how one client runs several jobs concurrently.
     */
    public JobProgress run(SubmitJobRequest request, Listener listener, long timeoutMs)
            throws InterruptedException, TimeoutException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        String lastNote = "";

        while (System.currentTimeMillis() < deadline) {
            CoordinatorInfo coordinator = findCoordinator();
            if (coordinator == null || !coordinator.getPresent()) {
                lastNote = note(listener, lastNote, "no coordinator right now (election in progress?) - waiting");
                Thread.sleep(500);
                continue;
            }
            final String who = "Worker " + coordinator.getNode().getId();
            ManagedChannel channel = channelFor(coordinator);
            JobServiceGrpc.JobServiceBlockingStub stub = JobServiceGrpc.newBlockingStub(channel);

            SubmitJobReply reply;
            try {
                reply = stub.withDeadlineAfter(10, TimeUnit.SECONDS).submitJob(request);
            } catch (StatusRuntimeException e) {
                lastNote = note(listener, lastNote, who + " is unreachable (" + e.getStatus().getCode() + ") - looking for the new coordinator");
                dropCoordinatorChannel();
                Thread.sleep(500);
                continue;
            }
            switch (reply.getStatus()) {
                case SUBMIT_STATUS_ACCEPTED:
                case SUBMIT_STATUS_ALREADY_KNOWN:
                    break;
                case SUBMIT_STATUS_INVALID:
                    throw new IllegalArgumentException(reply.getMessage());
                default:
                    lastNote = note(listener, lastNote, who + ": " + reply.getMessage() + " - retrying");
                    Thread.sleep(500);
                    continue;
            }

            try {
                long remaining = Math.max(1000, deadline - System.currentTimeMillis());
                Iterator<JobProgress> progress = stub.withDeadlineAfter(remaining, TimeUnit.MILLISECONDS)
                        .watchJob(WatchJobRequest.newBuilder().setJobId(request.getJobId()).build());
                while (progress.hasNext()) {
                    JobProgress p = progress.next();
                    listener.onProgress(p);
                    if (p.getState() != JobState.JOB_STATE_RUNNING) {
                        return p;
                    }
                }
            } catch (StatusRuntimeException e) {
                lastNote = note(listener, lastNote, "lost the coordinator while watching (" + e.getStatus().getCode()
                        + ") - finding the new one and re-attaching to the job");
                dropCoordinatorChannel();
                Thread.sleep(500);
            }
        }
        throw new TimeoutException("job " + request.getJobId() + " did not finish within " + timeoutMs / 1000 + " s");
    }

    /** Asks Bootstrap who the coordinator is; null if Bootstrap cannot be reached. */
    private CoordinatorInfo findCoordinator() {
        try {
            return bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS).getCoordinator(Empty.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            return null;
        }
    }

    private synchronized ManagedChannel channelFor(CoordinatorInfo c) {
        String key = c.getNode().getHost() + ":" + c.getNode().getPort();
        if (coordinatorChannel == null || !key.equals(coordinatorKey)) {
            dropCoordinatorChannel();
            coordinatorChannel = ManagedChannelBuilder.forAddress(c.getNode().getHost(), c.getNode().getPort())
                    .usePlaintext().maxInboundMessageSize(MAX_MESSAGE_BYTES).build();
            coordinatorKey = key;
        }
        return coordinatorChannel;
    }

    private synchronized void dropCoordinatorChannel() {
        if (coordinatorChannel != null) {
            coordinatorChannel.shutdownNow();
            coordinatorChannel = null;
            coordinatorKey = "";
        }
    }

    /** Tell the listener, but not the same thing twice in a row. */
    private static String note(Listener listener, String previous, String text) {
        if (!text.equals(previous)) {
            listener.onNote(text);
        }
        return text;
    }

    @Override
    public void close() {
        dropCoordinatorChannel();
        bootstrapChannel.shutdownNow();
    }
}
