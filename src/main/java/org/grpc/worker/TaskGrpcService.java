package org.grpc.worker;

import ds.grpc.CoordinatorInfo;
import ds.grpc.TaskRequest;
import ds.grpc.TaskServiceGrpc;
import ds.grpc.TaskUpdate;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker side of a task: runs it on its own thread and streams progress and checkpoints back to the
 * coordinator. Several tasks (from several jobs) can run on one worker at the same time, each on its
 * own Java thread. If the coordinator goes away the call is cancelled and the computation stops.
 */
public class TaskGrpcService extends TaskServiceGrpc.TaskServiceImplBase {

    private final WorkerNode worker;
    private final ExecutorService taskPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "task-thread");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger running = new AtomicInteger();

    public TaskGrpcService(WorkerNode worker) {
        this.worker = worker;
    }

    @Override
    public void executeTask(TaskRequest request, StreamObserver<TaskUpdate> out) {
        ServerCallStreamObserver<TaskUpdate> call = (ServerCallStreamObserver<TaskUpdate>) out;
        AtomicBoolean cancelled = new AtomicBoolean();
        call.setOnCancelHandler(() -> cancelled.set(true));

        // Fencing: refuse work from a coordinator that is older than the newest one we know.
        CoordinatorInfo known = worker.getCoordinator();
        if (known.getPresent() && request.getTerm() < known.getTerm()) {
            out.onError(Status.FAILED_PRECONDITION
                    .withDescription("stale coordinator term " + request.getTerm() + " (current " + known.getTerm() + ")")
                    .asRuntimeException());
            return;
        }

        int[] numbers = new int[request.getNumbersCount()];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = request.getNumbers(i);
        }
        taskPool.submit(() -> execute(request, numbers, call, cancelled));
    }

    private void execute(TaskRequest req, int[] numbers, ServerCallStreamObserver<TaskUpdate> call, AtomicBoolean cancelled) {
        int workerId = worker.getSelf().getId();
        int activeNow = running.incrementAndGet();
        String what = req.getType().name().replace("JOB_TYPE_", "")
                + (req.getNumbersCount() > 0 ? " on " + numbers.length + " numbers"
                : " on range " + req.getRangeStart() + "-" + req.getRangeEnd());
        log(workerId, "START task " + req.getTaskId() + " (" + what + ", attempt " + req.getAttempt()
                + (req.getResumeOffset() > 0 ? ", resuming after " + req.getResumeOffset() + " finished items" : "")
                + ") - tasks running on this worker now: " + activeNow);
        long started = System.nanoTime();
        try {
            boolean finished = TaskCompute.run(req.getType(), numbers, req.getRangeStart(), req.getRangeEnd(),
                    req.getResumeOffset(), req.getResumeValue(), cancelled::get,
                    (processed, partial, done) -> {
                        if (cancelled.get()) {
                            throw new CancellationException();
                        }
                        synchronized (call) {
                            call.onNext(TaskUpdate.newBuilder()
                                    .setTaskId(req.getTaskId()).setWorkerId(workerId)
                                    .setProcessed(processed).setTotal(TaskCompute.total(req.getType(), numbers,
                                            req.getRangeStart(), req.getRangeEnd()))
                                    .setPartialValue(partial).setFinished(done).build());
                        }
                    });
            if (finished) {
                synchronized (call) {
                    call.onCompleted();
                }
                log(workerId, "DONE  task " + req.getTaskId() + " in "
                        + (System.nanoTime() - started) / 1_000_000 + " ms");
            } else {
                log(workerId, "STOP  task " + req.getTaskId() + " (coordinator cancelled it)");
            }
        } catch (CancellationException | StatusRuntimeException e) {
            log(workerId, "STOP  task " + req.getTaskId() + " (coordinator went away)");
        } catch (RuntimeException e) {
            try {
                call.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
            } catch (RuntimeException ignored) {
                // the call is already closed
            }
        } finally {
            running.decrementAndGet();
        }
    }

    private static void log(int workerId, String message) {
        System.out.println("[Worker " + workerId + "] " + message);
    }
}
