
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClusterClient implements AutoCloseable {

    public interface Listener {
        void onProgress(JobProgress progress);

        void onNote(String note);
    }

    private static final int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    // Reconnect and check the job periodically instead of holding one
    // watch RPC open for the entire job timeout.
    private static final long WATCH_WINDOW_MS = 10_000;
    private static final long RETRY_DELAY_MS = 300;

    private final ManagedChannel bootstrapChannel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;

    private ManagedChannel coordinatorChannel;
    private String coordinatorKey = "";

    public ClusterClient(String bootstrapHost, int bootstrapPort) {
        bootstrapChannel = ManagedChannelBuilder
                .forAddress(bootstrapHost, bootstrapPort)
                .usePlaintext()
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .build();

        bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
    }

    /**
     * Submits a job and follows it until it reaches a terminal state.
     *
     * If the coordinator changes, the client discovers the current
     * coordinator and reattaches using the original job ID.
     *
     * timeoutMs remains the overall timeout for the operation.
     */
    public JobProgress run(
            SubmitJobRequest request,
            Listener listener,
            long timeoutMs)
            throws InterruptedException, TimeoutException {

        if (request == null) {
            throw new IllegalArgumentException("Job request cannot be null");
        }

        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }

        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }

        final long startedAt = System.nanoTime();
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);

        String lastNote = "";

        while (true) {
            long remainingNanos =
                    timeoutNanos - (System.nanoTime() - startedAt);

            if (remainingNanos <= 0) {
                throw new TimeoutException(
                        "Job " + request.getJobId()
                                + " did not finish within "
                                + timeoutMs / 1000 + " s");
            }

            // Discover the current coordinator on every reconnect cycle.
            CoordinatorInfo coordinator = findCoordinator();

            if (coordinator == null || !coordinator.getPresent()) {
                lastNote = note(
                        listener,
                        lastNote,
                        "No coordinator right now (election in progress?) - waiting");

                sleepBeforeRetry(remainingNanos);
                continue;
            }

            String who = "Worker " + coordinator.getNode().getId();

            ManagedChannel channel = channelFor(coordinator);

            JobServiceGrpc.JobServiceBlockingStub stub =
                    JobServiceGrpc.newBlockingStub(channel);

            /*
             * Re-submit the SAME request with the SAME job ID.
             *
             * The coordinator must recognize the existing job and return
             * ALREADY_KNOWN rather than create a duplicate.
             */
            SubmitJobReply reply;

            try {
                long remainingMs = Math.max(
                        1,
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos));

                reply = stub
                        .withDeadlineAfter(
                                Math.min(10_000, remainingMs),
                                TimeUnit.MILLISECONDS)
                        .submitJob(request);

            } catch (StatusRuntimeException e) {
                lastNote = note(
                        listener,
                        lastNote,
                        who + " is unreachable ("
                                + e.getStatus().getCode()
                                + ") - looking for the current coordinator");

                dropCoordinatorChannel();
                sleepBeforeRetry(remainingNanos);
                continue;
            }

            switch (reply.getStatus()) {
                case SUBMIT_STATUS_ACCEPTED:
                case SUBMIT_STATUS_ALREADY_KNOWN:
                    // Continue watching the existing job.
                    break;

                case SUBMIT_STATUS_INVALID:
                    throw new IllegalArgumentException(reply.getMessage());

                default:
                    lastNote = note(
                            listener,
                            lastNote,
                            who + ": " + reply.getMessage()
                                    + " - retrying");

                    sleepBeforeRetry(remainingNanos);
                    continue;
            }

            /*
             * Watch in short windows. This allows the client to periodically
             * check Bootstrap and reconnect if the coordinator has changed.
             */
            remainingNanos =
                    timeoutNanos - (System.nanoTime() - startedAt);

            if (remainingNanos <= 0) {
                continue;
            }

            long watchMs = Math.max(
                    1,
                    Math.min(
                            WATCH_WINDOW_MS,
                            TimeUnit.NANOSECONDS.toMillis(remainingNanos)));

            boolean receivedProgress = false;

            try {
                Iterator<JobProgress> progress = stub
                        .withDeadlineAfter(watchMs, TimeUnit.MILLISECONDS)
                        .watchJob(
                                WatchJobRequest.newBuilder()
                                        .setJobId(request.getJobId())
                                        .build());

                while (progress.hasNext()) {
                    JobProgress p = progress.next();
                    receivedProgress = true;

                    // Deliver progress updates to the CLI or GUI.
                    listener.onProgress(p);

                    // Only a terminal job state means the job has finished.
                    if (p.getState() != JobState.JOB_STATE_RUNNING) {
                        return p;
                    }

                    remainingNanos =
                            timeoutNanos - (System.nanoTime() - startedAt);

                    if (remainingNanos <= 0) {
                        throw new TimeoutException(
                                "Job " + request.getJobId()
                                        + " did not finish within "
                                        + timeoutMs / 1000 + " s");
                    }
                }

                /*
                 * A watch stream that ends normally without a terminal
                 * state is not proof that the job completed.
                 * Return to the outer loop and reattach.
                 */
                if (!receivedProgress) {
                    sleepBeforeRetry(
                            timeoutNanos - (System.nanoTime() - startedAt));
                }

            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();

                remainingNanos =
                        timeoutNanos - (System.nanoTime() - startedAt);

                /*
                 * DEADLINE_EXCEEDED can simply mean our 10-second watch
                 * window ended. Keep the channel and check the coordinator
                 * again; don't treat this alone as a coordinator failure.
                 */
                if (code == Status.Code.DEADLINE_EXCEEDED
                        && remainingNanos > 0) {

                    continue;
                }

                lastNote = note(
                        listener,
                        lastNote,
                        "Lost the coordinator while watching ("
                                + code
                                + ") - finding the current coordinator "
                                + "and reattaching to the same job");

                dropCoordinatorChannel();

                sleepBeforeRetry(remainingNanos);
            }
        }
    }

    /**
     * Asks Bootstrap for the current coordinator.
     * Returns null if Bootstrap cannot be reached.
     */
    private CoordinatorInfo findCoordinator() {
        try {
            return bootstrap
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .getCoordinator(Empty.getDefaultInstance());

        } catch (StatusRuntimeException e) {
            return null;
        }
    }

    /**
     * Reuses the channel if the coordinator endpoint has not changed.
     */
    private synchronized ManagedChannel channelFor(CoordinatorInfo c) {
        String key = c.getNode().getHost()
                + ":" + c.getNode().getPort();

        if (coordinatorChannel == null || !key.equals(coordinatorKey)) {
            dropCoordinatorChannel();

            coordinatorChannel = ManagedChannelBuilder
                    .forAddress(
                            c.getNode().getHost(),
                            c.getNode().getPort())
                    .usePlaintext()
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .build();

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

    private static void sleepBeforeRetry(long remainingNanos)
            throws InterruptedException {

        if (remainingNanos <= 0) {
            return;
        }

        long sleepMs = Math.min(
                RETRY_DELAY_MS,
                Math.max(
                        1,
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos)));

        Thread.sleep(sleepMs);
    }

    /**
     * Tell the listener about a change, avoiding repeated identical notes.
     */
    private static String note(
            Listener listener,
            String previous,
            String message) {

        if (!message.equals(previous)) {
            listener.onNote(message);
        }

        return message;
    }

    @Override
    public void close() {
        dropCoordinatorChannel();
        bootstrapChannel.shutdownNow();
    }
}
