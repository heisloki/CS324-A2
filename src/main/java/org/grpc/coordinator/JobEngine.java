
package org.grpc.coordinator;

import ds.grpc.CoordinatorInfo;
import ds.grpc.Empty;
import ds.grpc.FailureReport;
import ds.grpc.JobProgress;
import ds.grpc.JobRecord;
import ds.grpc.JobState;
import ds.grpc.JobType;
import ds.grpc.NodeInfo;
import ds.grpc.PingRequest;
import ds.grpc.ReplicaServiceGrpc;
import ds.grpc.StateSnapshot;
import ds.grpc.SubmitJobReply;
import ds.grpc.SubmitJobRequest;
import ds.grpc.SubmitStatus;
import ds.grpc.TaskProgress;
import ds.grpc.TaskRecord;
import ds.grpc.TaskRequest;
import ds.grpc.TaskServiceGrpc;
import ds.grpc.TaskState;
import ds.grpc.TaskUpdate;
import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import org.grpc.worker.ReplicaStore;
import org.grpc.worker.WorkerNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator job engine with replicated state and checkpoint recovery.
 *
 * The coordinator periodically replicates job/task state to other workers.
 * When a worker becomes coordinator, it fetches the best available snapshot,
 * restores jobs, and resumes unfinished tasks from their last checkpoints.
 */
public class JobEngine {

    public static final int JOBS_PER_TERM = 5;

    static final long WATCHDOG_INTERVAL_MS = 1000;
    static final long PING_DEADLINE_MS = 500;
    static final int MAX_MISSED_PINGS = 2;
    static final long SUSPECT_MS = 10_000;
    static final long WORKER_WAIT_PATIENCE_MS = 30_000;
    static final int MAX_TASK_ERRORS = 3;
    static final int KEEP_FINISHED_JOBS = 100;

    private enum Outcome {
        DONE, WORKER_FAILED, TASK_ERROR, STOP, STALE
    }

    private static final class Task {
        final String id;
        final long first;
        final long count;

        // Guarded by synchronized(job).
        TaskState state = TaskState.TASK_STATE_QUEUED;
        NodeInfo target;
        int attempt;
        long processed;
        long partial;
        long result;
        int errors;
        String cancelReason;
        Context.CancellableContext ctx;

        Task(String id, long first, long count) {
            this.id = id;
            this.first = first;
            this.count = count;
        }
    }

    private static final class Job {
        final String id;
        final JobType type;
        final int[] numbers;
        final int rangeStart;
        final int rangeEnd;
        final long total;

        // Updated on coordinator takeover so requests use the new fencing term.
        long term;

        final List<Task> tasks = new ArrayList<>();

        // Guarded by synchronized(job).
        JobState state = JobState.JOB_STATE_RUNNING;
        long result;
        String message = "";
        long doneAt;

        Job(
                String id,
                JobType type,
                int[] numbers,
                int rangeStart,
                int rangeEnd,
                long total,
                long term) {

            this.id = id;
            this.type = type;
            this.numbers = numbers;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.total = total;
            this.term = term;
        }
    }

    private final WorkerNode worker;
    private final ReplicaStore replicaStore;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    private final ExecutorService taskPool =
            Executors.newCachedThreadPool(daemon("job-dispatch"));

    private final ScheduledExecutorService watchdog =
            Executors.newSingleThreadScheduledExecutor(daemon("job-watchdog"));

    private final ScheduledExecutorService replicator =
            Executors.newSingleThreadScheduledExecutor(daemon("job-replicator"));

    private final AtomicLong snapshotSequence = new AtomicLong();
    private final Map<Integer, Long> suspectUntil = new ConcurrentHashMap<>();

    // Only touched by the watchdog thread.
    private final Map<Integer, Integer> missedPings = new HashMap<>();

    private final Object lock = new Object();

    private boolean active;
    private long term;
    private int jobsThisTerm;
    private boolean termFull;
    private int inflight;
    private boolean electionRequested;

    public JobEngine(WorkerNode worker, ReplicaStore replicaStore) {
        this.worker = worker;
        this.replicaStore = replicaStore;

        watchdog.scheduleWithFixedDelay(
                () -> {
                    try {
                        watchdogTick();
                    } catch (RuntimeException e) {
                        log("watchdog error: " + e);
                    }
                },
                WATCHDOG_INTERVAL_MS,
                WATCHDOG_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        replicator.scheduleWithFixedDelay(
                () -> {
                    try {
                        replicateSnapshot();
                    } catch (RuntimeException e) {
                        log("snapshot replication error: " + e);
                    }
                },
                250,
                500,
                TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------
    // Coordinator role changes and recovery
    // ------------------------------------------------------------------

    /**
     * Recover state before accepting new jobs. Replication is disabled while
     * active is false, so an empty snapshot cannot overwrite older replicas
     * before recovery has fetched them.
     */
    public void activate(long newTerm) {
        log("starting recovery for coordinator term " + newTerm);

        StateSnapshot bestSnapshot = fetchBestSnapshot(newTerm);

        if (bestSnapshot != null) {
            restoreSnapshot(bestSnapshot, newTerm);
        } else {
            log("no usable replicated snapshot found");
        }

        List<Job> toResume = new ArrayList<>();

        synchronized (lock) {
            term = newTerm;
            jobsThisTerm = 0;
            termFull = false;
            electionRequested = false;
            inflight = 0;

            // Existing in-memory jobs may survive if this worker was an
            // earlier coordinator. Preserve their latest local checkpoints.
            for (Job job : jobs.values()) {
                synchronized (job) {
                    if (job.state != JobState.JOB_STATE_RUNNING) {
                        continue;
                    }

                    job.term = newTerm;

                    for (Task task : job.tasks) {
                        if (task.state != TaskState.TASK_STATE_COMPLETED) {
                            // Any old stream belonged to the previous term.
                            task.state = TaskState.TASK_STATE_QUEUED;
                            task.target = null;
                            task.cancelReason = null;
                            task.ctx = null;
                        }
                    }

                    inflight++;
                    toResume.add(job);
                }
            }

            active = true;
        }

        log("accepting jobs for term " + newTerm
                + " (up to " + JOBS_PER_TERM + " new jobs this term); "
                + toResume.size() + " unfinished job(s) to resume");

        // Schedule only tasks that are not already completed.
        for (Job job : toResume) {
            boolean hasUnfinished = false;

            synchronized (job) {
                for (Task task : job.tasks) {
                    if (task.state != TaskState.TASK_STATE_COMPLETED) {
                        hasUnfinished = true;
                        taskPool.submit(() -> runTask(job, task, null));
                    }
                }
            }

            // A snapshot can be taken after every task finishes but before
            // the job's final state is replicated.
            if (!hasUnfinished) {
                checkCompletion(job);
            }
        }
    }

    /**
     * Read the local replica first, then ask reachable peers for their latest
     * stored snapshots. Prefer the highest term, then the highest sequence.
     */
    private StateSnapshot fetchBestSnapshot(long newTerm) {
        StateSnapshot best = replicaStore.getSnapshot();

        if (!usableSnapshot(best, newTerm)) {
            best = null;
        }

        for (NodeInfo node : liveWorkers()) {
            if (node.getId() == selfId()) {
                continue;
            }

            try {
                StateSnapshot candidate =
                        ReplicaServiceGrpc.newBlockingStub(
                                        worker.connections().channel(node))
                                .withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                                .fetchState(Empty.getDefaultInstance());

                if (!usableSnapshot(candidate, newTerm)) {
                    continue;
                }

                if (best == null || newerSnapshot(candidate, best)) {
                    best = candidate;
                }
            } catch (StatusRuntimeException e) {
                log("could not fetch replica from Worker "
                        + node.getId() + ": " + e.getStatus().getCode());
            }
        }

        if (best != null) {
            log("selected snapshot from coordinator Worker "
                    + best.getCoordinatorId()
                    + " (term " + best.getTerm()
                    + ", sequence " + best.getSequence()
                    + ", jobs " + best.getJobsCount() + ")");
        }

        return best;
    }

    private boolean usableSnapshot(StateSnapshot snapshot, long newTerm) {
        return snapshot != null
                && snapshot.getCoordinatorId() > 0
                && snapshot.getTerm() <= newTerm;
    }

    private static boolean newerSnapshot(
            StateSnapshot candidate,
            StateSnapshot current) {

        if (candidate.getTerm() != current.getTerm()) {
            return candidate.getTerm() > current.getTerm();
        }

        return candidate.getSequence() > current.getSequence();
    }

    /**
     * Restore jobs from a snapshot. Existing local jobs are retained because
     * they may have checkpoints newer than the last snapshot they replicated.
     */
    private void restoreSnapshot(StateSnapshot snapshot, long newTerm) {
        int restored = 0;

        for (JobRecord record : snapshot.getJobsList()) {
            if (record.getJobId().isBlank()
                    || jobs.containsKey(record.getJobId())) {
                continue;
            }

            int[] numbers = null;

            if (record.getNumbersCount() > 0) {
                numbers = new int[record.getNumbersCount()];

                for (int i = 0; i < numbers.length; i++) {
                    numbers[i] = record.getNumbers(i);
                }
            }

            long total = 0;

            for (TaskRecord taskRecord : record.getTasksList()) {
                total += Math.max(0, taskRecord.getTotal());
            }

            if (total == 0) {
                total = record.getType() == JobType.JOB_TYPE_PRIMESUM
                        ? (long) record.getRangeEnd()
                                - record.getRangeStart() + 1
                        : (numbers == null ? 0 : numbers.length);
            }

            Job job = new Job(
                    record.getJobId(),
                    record.getType(),
                    numbers,
                    record.getRangeStart(),
                    record.getRangeEnd(),
                    total,
                    newTerm);

            job.result = record.getResultValue();
            job.state = record.getState();

            if (job.state == JobState.JOB_STATE_COMPLETED) {
                job.message = "completed (recovered result)";
                job.doneAt = System.currentTimeMillis();
            } else if (job.state == JobState.JOB_STATE_FAILED) {
                job.message = "failed (recovered state)";
                job.doneAt = System.currentTimeMillis();
            } else {
                job.state = JobState.JOB_STATE_RUNNING;
                job.message = "Recovered after coordinator change; resuming from checkpoints";
            }

            for (TaskRecord taskRecord : record.getTasksList()) {
                long count = Math.max(0, taskRecord.getTotal());

                if (count == 0) {
                    continue;
                }

                Task task = new Task(
                        taskRecord.getTaskId(),
                        taskRecord.getItemStart(),
                        count);

                task.attempt = taskRecord.getAttempt();
                task.processed = Math.max(
                        0, Math.min(taskRecord.getProcessed(), count));
                task.partial = taskRecord.getPartialValue();
                task.result = taskRecord.getResultValue();

                if (taskRecord.getState()
                        == TaskState.TASK_STATE_COMPLETED) {

                    task.state = TaskState.TASK_STATE_COMPLETED;
                    task.processed = count;
                } else {
                    // A task that was running on the old coordinator is
                    // resumed by sending its last checkpoint to a worker.
                    task.state = TaskState.TASK_STATE_QUEUED;
                    task.target = null;
                }

                job.tasks.add(task);
            }

            // Do not replace a job that arrived locally while this snapshot
            // was being fetched.
            if (jobs.putIfAbsent(job.id, job) == null) {
                restored++;
            }
        }

        log("restored " + restored + " job(s) from replicated state");
    }

    /**
     * Stop dispatching work when this worker loses the coordinator role.
     * Preserve the last checkpoints and mark unfinished tasks as queued.
     */
    public void deactivate(String why) {
        synchronized (lock) {
            if (!active) {
                return;
            }

            active = false;
            inflight = 0;
        }

        int retained = 0;

        for (Job job : jobs.values()) {
            synchronized (job) {
                if (job.state != JobState.JOB_STATE_RUNNING) {
                    continue;
                }

                retained++;

                for (Task task : job.tasks) {
                    task.cancelReason = "STEP_DOWN";

                    if (task.ctx != null) {
                        task.ctx.cancel(null);
                    }

                    if (task.state != TaskState.TASK_STATE_COMPLETED) {
                        task.state = TaskState.TASK_STATE_QUEUED;
                        task.target = null;
                    }
                }
            }
        }

        log("no longer coordinator (" + why + ")"
                + (retained == 0
                ? ""
                : "; retained " + retained + " unfinished job(s)"));
    }

    // ------------------------------------------------------------------
    // Job submission
    // ------------------------------------------------------------------

    public SubmitJobReply submit(SubmitJobRequest r) {
        final String id = r.getJobId();

        if (id.isBlank()) {
            return reply(
                    SubmitStatus.SUBMIT_STATUS_INVALID,
                    "job_id is required",
                    id);
        }

        synchronized (lock) {
            if (!active || !worker.isCoordinator()) {
                return reply(
                        SubmitStatus.SUBMIT_STATUS_NOT_COORDINATOR,
                        "this worker is not the coordinator",
                        id);
            }

            if (jobs.containsKey(id)) {
                return reply(
                        SubmitStatus.SUBMIT_STATUS_ALREADY_KNOWN,
                        "job already known - just watch it",
                        id);
            }
        }

        String problem = validate(r);

        if (problem != null) {
            return reply(
                    SubmitStatus.SUBMIT_STATUS_INVALID,
                    problem,
                    id);
        }

        List<NodeInfo> workers = liveWorkers();

        if (workers.isEmpty()) {
            return reply(
                    SubmitStatus.SUBMIT_STATUS_NO_WORKERS,
                    "no live workers",
                    id);
        }

        final Job job;

        synchronized (lock) {
            if (!active || !worker.isCoordinator()) {
                return reply(
                        SubmitStatus.SUBMIT_STATUS_NOT_COORDINATOR,
                        "this worker is not the coordinator",
                        id);
            }

            if (jobs.containsKey(id)) {
                return reply(
                        SubmitStatus.SUBMIT_STATUS_ALREADY_KNOWN,
                        "job already known - just watch it",
                        id);
            }

            if (jobsThisTerm >= JOBS_PER_TERM) {
                return reply(
                        SubmitStatus.SUBMIT_STATUS_TERM_EXHAUSTED,
                        "this term's " + JOBS_PER_TERM
                                + " jobs are used up - a new coordinator is being elected",
                        id);
            }

            jobsThisTerm++;
            inflight++;

            if (jobsThisTerm >= JOBS_PER_TERM) {
                termFull = true;
            }

            job = buildJob(r, workers);
            jobs.put(id, job);

            log("job " + shortId(id) + " " + label(job.type)
                    + " accepted: " + job.tasks.size()
                    + " task(s) over workers "
                    + workers.stream().map(NodeInfo::getId).toList()
                    + " (job " + jobsThisTerm + " of "
                    + JOBS_PER_TERM + " this term)");
        }

        for (int i = 0; i < job.tasks.size(); i++) {
            final Task task = job.tasks.get(i);
            final NodeInfo first = workers.get(i % workers.size());

            taskPool.submit(() -> runTask(job, task, first));
        }

        return reply(
                SubmitStatus.SUBMIT_STATUS_ACCEPTED,
                "accepted",
                id);
    }

    private static String validate(SubmitJobRequest r) {
        switch (r.getType()) {
            case JOB_TYPE_MAX:
            case JOB_TYPE_PRIMECOUNT:
                return r.getNumbersCount() == 0
                        ? "numbers must not be empty"
                        : null;

            case JOB_TYPE_PRIMESUM:
                return r.getRangeEnd() < r.getRangeStart()
                        ? "range_end must not be smaller than range_start"
                        : null;

            default:
                return "unknown job type";
        }
    }

    /** Divide work as evenly as possible, creating at most one task per worker. */
    private Job buildJob(SubmitJobRequest r, List<NodeInfo> workers) {
        final boolean isRange =
                r.getType() == JobType.JOB_TYPE_PRIMESUM;

        int[] numbers = null;

        if (!isRange) {
            numbers = new int[r.getNumbersCount()];

            for (int i = 0; i < numbers.length; i++) {
                numbers[i] = r.getNumbers(i);
            }
        }

        long total = isRange
                ? (long) r.getRangeEnd() - r.getRangeStart() + 1
                : numbers.length;

        Job job = new Job(
                r.getJobId(),
                r.getType(),
                numbers,
                r.getRangeStart(),
                r.getRangeEnd(),
                total,
                term);

        int n = (int) Math.min(workers.size(), total);

        if (n <= 0) {
            throw new IllegalArgumentException("Job has no work items");
        }

        long base = total / n;
        long extra = total % n;
        long cursor = 0;

        for (int k = 0; k < n; k++) {
            long count = base + (k < extra ? 1 : 0);
            long first = isRange
                    ? r.getRangeStart() + cursor
                    : cursor;

            job.tasks.add(new Task(
                    job.id + "-t" + (k + 1),
                    first,
                    count));

            cursor += count;
        }

        return job;
    }

    // ------------------------------------------------------------------
    // Running one task with worker-failure recovery
    // ------------------------------------------------------------------

    private void runTask(Job job, Task task, NodeInfo firstTarget) {
        NodeInfo target = firstTarget;
        long waitingSince = 0;

        while (true) {
            synchronized (job) {
                if (job.state != JobState.JOB_STATE_RUNNING) {
                    return;
                }
            }

            if (!isActive()) {
                return;
            }

            if (target == null) {
                target = pickReplacement();

                if (target == null) {
                    long now = System.currentTimeMillis();

                    if (waitingSince == 0) {
                        waitingSince = now;
                    }

                    if (now - waitingSince > WORKER_WAIT_PATIENCE_MS) {
                        failJob(
                                job,
                                "no worker was available to run task " + task.id);
                        return;
                    }

                    sleepQuietly(1000);
                    continue;
                }

                waitingSince = 0;
            }

            final TaskRequest request;

            synchronized (job) {
                if (job.state != JobState.JOB_STATE_RUNNING) {
                    return;
                }

                task.attempt++;
                task.target = target;
                task.state = TaskState.TASK_STATE_RUNNING;
                task.cancelReason = null;

                request = buildRequest(job, task);

                if (task.attempt > 1) {
                    job.message = "Task " + shortTask(task)
                            + " reassigned to Worker " + target.getId()
                            + ", resuming at "
                            + percent(task.processed, task.count)
                            + "% from its checkpoint";
                }
            }

            if (target.getId() != selfId()) {
                worker.incrementJac(1);
            }

            log("task " + task.id + " -> Worker " + target.getId()
                    + " (attempt " + request.getAttempt()
                    + (request.getResumeOffset() > 0
                    ? ", resuming after " + request.getResumeOffset() + " items"
                    : "")
                    + ")");

            Outcome outcome = stream(job, task, target, request);

            switch (outcome) {
                case DONE:
                    checkCompletion(job);
                    return;

                case STOP:
                    return;

                case STALE:
                    deactivate("a coordinator with a newer term exists");
                    return;

                case TASK_ERROR: {
                    int errors;

                    synchronized (job) {
                        errors = ++task.errors;
                        task.state = TaskState.TASK_STATE_QUEUED;
                    }

                    if (errors >= MAX_TASK_ERRORS) {
                        failJob(
                                job,
                                "task " + task.id + " failed " + errors + " times");
                        return;
                    }

                    target = null;
                    break;
                }

                case WORKER_FAILED:
                default:
                    workerFailed(job, task, target);
                    target = null;
                    break;
            }
        }
    }

    /** Stream task progress and checkpoints from the selected worker. */
    private Outcome stream(
            Job job,
            Task task,
            NodeInfo target,
            TaskRequest request) {

        Context.CancellableContext ctx =
                Context.current().withCancellation();

        synchronized (job) {
            task.ctx = ctx;
        }

        try {
            return ctx.call(() -> {
                TaskServiceGrpc.TaskServiceBlockingStub stub =
                        TaskServiceGrpc.newBlockingStub(
                                worker.connections().channel(target));

                Iterator<TaskUpdate> updates = stub.executeTask(request);

                while (updates.hasNext()) {
                    TaskUpdate update = updates.next();

                    applyUpdate(job, task, update);

                    if (update.getFinished()) {
                        return Outcome.DONE;
                    }
                }

                return Outcome.WORKER_FAILED;
            });
        } catch (StatusRuntimeException e) {
            return classify(job, task, e);
        } catch (Exception e) {
            return Outcome.WORKER_FAILED;
        } finally {
            ctx.cancel(null);

            synchronized (job) {
                if (task.ctx == ctx) {
                    task.ctx = null;
                }
            }
        }
    }

    private Outcome classify(
            Job job,
            Task task,
            StatusRuntimeException e) {

        String reason;

        synchronized (job) {
            reason = task.cancelReason;
        }

        if ("STEP_DOWN".equals(reason) || "JOB_FAILED".equals(reason)) {
            return Outcome.STOP;
        }

        switch (e.getStatus().getCode()) {
            case UNAVAILABLE:
            case DEADLINE_EXCEEDED:
            case CANCELLED:
            case ABORTED:
                return Outcome.WORKER_FAILED;

            case FAILED_PRECONDITION:
                return Outcome.STALE;

            default:
                log("task " + task.id + " error: " + e.getStatus());
                return Outcome.TASK_ERROR;
        }
    }

    private void applyUpdate(Job job, Task task, TaskUpdate update) {
        synchronized (job) {
            if (job.state != JobState.JOB_STATE_RUNNING) {
                return;
            }

            // Ignore out-of-order progress updates that move backwards.
            if (update.getProcessed() >= task.processed) {
                task.processed = Math.min(update.getProcessed(), task.count);
                task.partial = update.getPartialValue();
            }

            task.state = TaskState.TASK_STATE_RUNNING;

            if (update.getFinished()) {
                task.state = TaskState.TASK_STATE_COMPLETED;
                task.processed = task.count;
                task.result = update.getPartialValue();
            }
        }
    }

    /** Requeue a failed worker's task so it can resume from its last checkpoint. */
    private void workerFailed(Job job, Task task, NodeInfo dead) {
        int pct;

        synchronized (job) {
            task.state = TaskState.TASK_STATE_QUEUED;
            task.target = null;
            pct = percent(task.processed, task.count);

            job.message = "Worker " + dead.getId()
                    + " failed - task " + shortTask(task)
                    + " was at " + pct + "%, reassigning it";
        }

        suspectUntil.put(
                dead.getId(),
                System.currentTimeMillis() + SUSPECT_MS);

        worker.connections().forget(dead);

        log("Worker " + dead.getId()
                + " FAILED - task " + task.id
                + " was at " + pct + "%, reassigning");

        taskPool.submit(() -> {
            try {
                worker.bootstrapStub()
                        .withDeadlineAfter(3, TimeUnit.SECONDS)
                        .reportFailure(
                                FailureReport.newBuilder()
                                        .setReporterId(selfId())
                                        .setSuspectId(dead.getId())
                                        .setReason("task stream failed")
                                        .build());
            } catch (RuntimeException ignored) {
                // Bootstrap's health checks can also detect failed workers.
            }
        });
    }

/** Must be called while synchronized(job). */
    private TaskRequest buildRequest(Job job, Task task) {
        TaskRequest.Builder builder = TaskRequest.newBuilder()
                .setJobId(job.id)
                .setTaskId(task.id)
                // Use the term assigned to this job during recovery.
                .setTerm(job.term)
                .setType(job.type)
                .setAttempt(task.attempt)
                .setResumeOffset(task.processed)
                .setResumeValue(task.partial);

        if (job.type == JobType.JOB_TYPE_PRIMESUM) {
            builder.setRangeStart((int) task.first)
                    .setRangeEnd((int) (task.first + task.count - 1));
        } else {
            for (long i = task.first; i < task.first + task.count; i++) {
                builder.addNumbers(job.numbers[(int) i]);
            }
        }

        return builder.build();
    }

    // ------------------------------------------------------------------
    // Finishing a job
    // ------------------------------------------------------------------

    private void checkCompletion(Job job) {
        boolean finished = false;

        synchronized (job) {
            if (job.state != JobState.JOB_STATE_RUNNING) {
                return;
            }

            long result = job.type == JobType.JOB_TYPE_MAX
                    ? Long.MIN_VALUE
                    : 0;

            for (Task task : job.tasks) {
                if (task.state != TaskState.TASK_STATE_COMPLETED) {
                    return;
                }

                result = job.type == JobType.JOB_TYPE_MAX
                        ? Math.max(result, task.result)
                        : result + task.result;
            }

            job.result = result;
            job.state = JobState.JOB_STATE_COMPLETED;
            job.doneAt = System.currentTimeMillis();
            job.message = "completed";
            finished = true;
        }

        if (finished) {
            log("job " + shortId(job.id)
                    + " COMPLETED: " + label(job.type)
                    + " = " + job.result);

            onJobFinished();
        }
    }

    private void failJob(Job job, String why) {
        synchronized (job) {
            if (job.state != JobState.JOB_STATE_RUNNING) {
                return;
            }

            job.state = JobState.JOB_STATE_FAILED;
            job.message = why;
            job.doneAt = System.currentTimeMillis();

            for (Task task : job.tasks) {
                task.cancelReason = "JOB_FAILED";

                if (task.ctx != null) {
                    task.ctx.cancel(null);
                }
            }
        }

        log("job " + shortId(job.id) + " FAILED: " + why);

        onJobFinished();
    }

    /** Start a new election after all five new jobs for this term have finished. */
    private void onJobFinished() {
        boolean startElection = false;

        synchronized (lock) {
            inflight = Math.max(0, inflight - 1);

            if (termFull
                    && inflight == 0
                    && active
                    && !electionRequested) {

                electionRequested = true;
                startElection = true;
            }
        }

        pruneFinishedJobs();

        if (startElection) {
            log("all " + JOBS_PER_TERM
                    + " jobs of this term are done - ending the term and starting a new election");

            taskPool.submit(() ->
                    worker.startElection(
                            "term limit reached after " + JOBS_PER_TERM + " jobs"));
        }
    }

    private void pruneFinishedJobs() {
        if (jobs.size() <= KEEP_FINISHED_JOBS) {
            return;
        }

        jobs.values().stream()
                .filter(job -> {
                    synchronized (job) {
                        return job.state != JobState.JOB_STATE_RUNNING;
                    }
                })
                .sorted(Comparator.comparingLong(job -> {
                    synchronized (job) {
                        return job.doneAt;
                    }
                }))
                .limit(jobs.size() - KEEP_FINISHED_JOBS)
                .forEach(job -> jobs.remove(job.id));
    }

    // ------------------------------------------------------------------
    // Replicated coordinator state
    // ------------------------------------------------------------------

    private void replicateSnapshot() {
        final long snapshotTerm;
        final int currentJobsThisTerm;

        synchronized (lock) {
            if (!active || !worker.isCoordinator()) {
                return;
            }

            snapshotTerm = term;
            currentJobsThisTerm = jobsThisTerm;
        }

        StateSnapshot.Builder snapshot = StateSnapshot.newBuilder()
                .setTerm(snapshotTerm)
                .setCoordinatorId(selfId())
                .setSequence(snapshotSequence.incrementAndGet())
                .setJobsThisTerm(currentJobsThisTerm);

        for (Job job : jobs.values()) {
            synchronized (job) {
                JobRecord.Builder record = JobRecord.newBuilder()
                        .setJobId(job.id)
                        .setType(job.type)
                        .setRangeStart(job.rangeStart)
                        .setRangeEnd(job.rangeEnd)
                        .setState(job.state)
                        .setResultValue(job.result);

                if (job.numbers != null) {
                    for (int number : job.numbers) {
                        record.addNumbers(number);
                    }
                }

                for (Task task : job.tasks) {
                    TaskRecord taskRecord = TaskRecord.newBuilder()
                            .setTaskId(task.id)
                            .setState(task.state)
                            .setWorkerId(
                                    task.target == null
                                            ? 0
                                            : task.target.getId())
                            .setAttempt(task.attempt)
                            .setItemStart(task.first)
                            .setItemEnd(task.first + task.count - 1)
                            .setTotal(task.count)
                            .setProcessed(task.processed)
                            .setPartialValue(task.partial)
                            .setResultValue(task.result)
                            .build();

                    record.addTasks(taskRecord);
                }

                snapshot.addJobs(record.build());
            }
        }

        StateSnapshot completedSnapshot = snapshot.build();

        for (NodeInfo node : liveWorkers()) {
            if (node.getId() == selfId()) {
                continue;
            }

            try {
                ReplicaServiceGrpc.newBlockingStub(
                                worker.connections().channel(node))
                        .withDeadlineAfter(700, TimeUnit.MILLISECONDS)
                        .replicateState(completedSnapshot);

            } catch (StatusRuntimeException e) {
                log("could not replicate snapshot to Worker "
                        + node.getId() + ": "
                        + e.getStatus().getCode());
            }
        }
    }

    // ------------------------------------------------------------------
    // Progress for clients
    // ------------------------------------------------------------------

    /** Return the job's current progress, or null if this coordinator doesn't know it. */
    public JobProgress snapshot(String jobId) {
        Job job = jobs.get(jobId);

        if (job == null) {
            return null;
        }

        synchronized (job) {
            long done = 0;
            JobProgress.Builder builder = JobProgress.newBuilder();

            for (Task task : job.tasks) {
                long processed =
                        task.state == TaskState.TASK_STATE_COMPLETED
                                ? task.count
                                : task.processed;

                done += processed;

                builder.addTasks(
                        TaskProgress.newBuilder()
                                .setTaskId(task.id)
                                .setWorkerId(
                                        task.state == TaskState.TASK_STATE_QUEUED
                                                || task.target == null
                                                ? 0
                                                : task.target.getId())
                                .setState(task.state)
                                .setProcessed(processed)
                                .setTotal(task.count)
                                .setPercentComplete(
                                        round1(processed * 100.0 / task.count))
                                .setAttempt(task.attempt));
            }

            builder.setJobId(job.id)
                    .setState(job.state)
                    .setPercentComplete(
                            job.state == JobState.JOB_STATE_COMPLETED
                                    ? 100.0
                                    : job.total == 0
                                    ? 0.0
                                    : round1(done * 100.0 / job.total))
                    .setProcessedItems(done)
                    .setTotalItems(job.total)
                    .setCoordinatorId(selfId())
                    .setTerm(job.term)
                    .setMessage(job.message);

            if (job.state == JobState.JOB_STATE_COMPLETED) {
                builder.setHasResult(true)
                        .setResultValue(job.result)
                        .setResultText(
                                label(job.type) + " = " + job.result);
            }

            return builder.build();
        }
    }

    // ------------------------------------------------------------------
    // Heartbeats to workers running tasks
    // ------------------------------------------------------------------

    private void watchdogTick() {
        if (!isActive()) {
            return;
        }

        Map<Integer, NodeInfo> busy = new HashMap<>();

        for (Job job : jobs.values()) {
            synchronized (job) {
                if (job.state != JobState.JOB_STATE_RUNNING) {
                    continue;
                }

                for (Task task : job.tasks) {
                    if (task.state == TaskState.TASK_STATE_RUNNING
                            && task.target != null
                            && task.target.getId() != selfId()) {

                        busy.put(task.target.getId(), task.target);
                    }
                }
            }
        }

        missedPings.keySet().retainAll(busy.keySet());

        for (NodeInfo node : busy.values()) {
            boolean alive;

            try {
                worker.connections()
                        .peer(node)
                        .withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                        .ping(PingRequest.newBuilder()
                                .setSenderId(selfId())
                                .build());

                alive = true;

            } catch (StatusRuntimeException e) {
                alive = false;
            }

            if (alive) {
                missedPings.remove(node.getId());
                continue;
            }

            int missed = missedPings.merge(node.getId(), 1, Integer::sum);

            log("heartbeat to Worker " + node.getId()
                    + " missed (" + missed + "/" + MAX_MISSED_PINGS + ")");

            if (missed >= MAX_MISSED_PINGS) {
                missedPings.remove(node.getId());
                cancelTasksOn(node.getId());
            }
        }
    }

    /** Cancel streams assigned to a worker that stopped answering heartbeats. */
    private void cancelTasksOn(int workerId) {
        for (Job job : jobs.values()) {
            synchronized (job) {
                for (Task task : job.tasks) {
                    if (task.state == TaskState.TASK_STATE_RUNNING
                            && task.target != null
                            && task.target.getId() == workerId) {

                        task.cancelReason = "WORKER_DEAD";

                        if (task.ctx != null) {
                            task.ctx.cancel(
                                    new RuntimeException(
                                            "worker " + workerId
                                                    + " stopped answering heartbeats"));
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Choosing workers
    // ------------------------------------------------------------------

    /** Find live workers using Bootstrap or, if unavailable, known neighbours. */
    private List<NodeInfo> liveWorkers() {
        List<NodeInfo> list;

        try {
            list = new ArrayList<>(
                    worker.bootstrapStub()
                            .withDeadlineAfter(2, TimeUnit.SECONDS)
                            .getWorkers(Empty.getDefaultInstance())
                            .getWorkersList());

        } catch (StatusRuntimeException e) {
            log("Bootstrap unreachable (" + e.getStatus().getCode()
                    + ") - using my neighbours instead");

            list = new ArrayList<>(worker.neighborList());
        }

        final NodeInfo self = worker.getSelf();

        if (self != null
                && list.stream().noneMatch(n -> n.getId() == self.getId())) {
            list.add(self);
        }

        final long now = System.currentTimeMillis();

        list.removeIf(node ->
                self != null
                        && node.getId() != self.getId()
                        && suspectUntil.getOrDefault(node.getId(), 0L) > now);

        list.sort(Comparator.comparingInt(NodeInfo::getId));

        return list;
    }

    /** Choose the live worker running the fewest tasks. */
    private NodeInfo pickReplacement() {
        List<NodeInfo> candidates = liveWorkers();

        NodeInfo best = null;
        int bestLoad = Integer.MAX_VALUE;

        for (NodeInfo node : candidates) {
            int load = runningOn(node.getId());

            if (load < bestLoad) {
                best = node;
                bestLoad = load;
            }
        }

        return best;
    }

    private int runningOn(int workerId) {
        int count = 0;

        for (Job job : jobs.values()) {
            synchronized (job) {
                for (Task task : job.tasks) {
                    if (task.state == TaskState.TASK_STATE_RUNNING
                            && task.target != null
                            && task.target.getId() == workerId) {
                        count++;
                    }
                }
            }
        }

        return count;
    }

    // ------------------------------------------------------------------
    // Small helpers
    // ------------------------------------------------------------------

    private boolean isActive() {
        synchronized (lock) {
            return active;
        }
    }

    private SubmitJobReply reply(
            SubmitStatus status,
            String message,
            String jobId) {

        CoordinatorInfo hint = worker.getCoordinator();

        return SubmitJobReply.newBuilder()
                .setStatus(status)
                .setMessage(message)
                .setJobId(jobId)
                .setCoordinatorHint(hint)
                .build();
    }

    private int selfId() {
        NodeInfo self = worker.getSelf();
        return self == null ? 0 : self.getId();
    }

    private static int percent(long processed, long total) {
        return total == 0 ? 100 : (int) (processed * 100 / total);
    }

    private static double round1(double value) {
        return Math.round(value * 10) / 10.0;
    }

    private static String label(JobType type) {
        return type.name().replace("JOB_TYPE_", "");
    }

    private static String shortId(String id) {
        return id.length() > 8 ? id.substring(0, 8) : id;
    }

    private static String shortTask(Task task) {
        int dash = task.id.lastIndexOf('-');
        return dash >= 0 ? task.id.substring(dash + 1) : task.id;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(String message) {
        System.out.println(
                "[Worker " + selfId() + " | COORDINATOR] " + message);
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }
}
