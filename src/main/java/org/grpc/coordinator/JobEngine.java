package org.grpc.coordinator;

import ds.grpc.CoordinatorInfo;
import ds.grpc.Empty;
import ds.grpc.FailureReport;
import ds.grpc.JobProgress;
import ds.grpc.JobState;
import ds.grpc.JobType;
import ds.grpc.NodeInfo;
import ds.grpc.PingRequest;
import ds.grpc.SubmitJobReply;
import ds.grpc.SubmitJobRequest;
import ds.grpc.SubmitStatus;
import ds.grpc.TaskProgress;
import ds.grpc.TaskRequest;
import ds.grpc.TaskServiceGrpc;
import ds.grpc.TaskState;
import ds.grpc.TaskUpdate;
import io.grpc.Context;
import io.grpc.StatusRuntimeException;
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

/**
 * The coordinator's brain: accepts jobs, splits them into tasks, hands the tasks to workers, follows
 * their real progress, and keeps the job alive when a worker dies.
 *
 * <p>Failure handling (worker side), in short:
 * <ul>
 *   <li>Every task is a gRPC stream. If the stream breaks, or the worker stops answering heartbeat
 *       pings (checked every second, two misses in a row), the worker is declared failed.</li>
 *   <li>The task goes back to the queue and is given to another live worker, which RESUMES from the last
 *       checkpoint the failed worker streamed (items finished + running result). Finished work is never redone.</li>
 *   <li>Every task has a unique id and its result is stored under that id, so a late or duplicate
 *       answer can never be counted twice.</li>
 * </ul>
 */
public class JobEngine {

    /** Spec: a coordinator serves one term, during which it may assign up to five jobs. */
    public static final int JOBS_PER_TERM = 5;

    static final long WATCHDOG_INTERVAL_MS = 1000;
    static final long PING_DEADLINE_MS = 500;
    static final int MAX_MISSED_PINGS = 2;
    static final long SUSPECT_MS = 10_000;               // do not give work to a worker that just failed
    static final long WORKER_WAIT_PATIENCE_MS = 30_000;  // how long a task waits for any worker before the job fails
    static final int MAX_TASK_ERRORS = 3;                // non-network failures of one task before the job fails
    static final int KEEP_FINISHED_JOBS = 100;

    private enum Outcome { DONE, WORKER_FAILED, TASK_ERROR, STOP, STALE }

    private static final class Task {
        final String id;
        final long first;   // first item: list index (MAX/PRIMECOUNT) or first number (PRIMESUM)
        final long count;   // how many items
        // everything below is guarded by synchronized(job)
        TaskState state = TaskState.TASK_STATE_QUEUED;
        NodeInfo target;
        int attempt;
        long processed;     // CHECKPOINT: items finished ...
        long partial;       // ... and the running result at that point
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
        final long term;
        final List<Task> tasks = new ArrayList<>();
        // guarded by synchronized(job)
        JobState state = JobState.JOB_STATE_RUNNING;
        long result;
        String message = "";
        long doneAt;

        Job(String id, JobType type, int[] numbers, int rangeStart, int rangeEnd, long total, long term) {
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
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final ExecutorService taskPool = Executors.newCachedThreadPool(daemon("job-dispatch"));
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(daemon("job-watchdog"));
    private final Map<Integer, Long> suspectUntil = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> missedPings = new HashMap<>(); // only touched by the watchdog thread

    private final Object lock = new Object(); // guards the term bookkeeping below
    private boolean active;
    private long term;
    private int jobsThisTerm;
    private boolean termFull;
    private int inflight;
    private boolean electionRequested;

    public JobEngine(WorkerNode worker) {
        this.worker = worker;
        watchdog.scheduleWithFixedDelay(() -> {
            try {
                watchdogTick();
            } catch (RuntimeException e) {
                log("watchdog error: " + e);
            }
        }, WATCHDOG_INTERVAL_MS, WATCHDOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------ role changes (called by ClusterWorker)

    /** This worker just won the election for {@code newTerm}: start accepting jobs. */
    public void activate(long newTerm) {
        synchronized (lock) {
            active = true;
            term = newTerm;
            jobsThisTerm = 0;
            termFull = false;
            inflight = 0;
            electionRequested = false;
        }
        log("accepting jobs for term " + newTerm + " (up to " + JOBS_PER_TERM + " jobs this term)");
    }

    /** A newer coordinator exists: stop. Unfinished jobs are dropped here; the new coordinator takes over (step 3). */
    public void deactivate(String why) {
        synchronized (lock) {
            if (!active) {
                return;
            }
            active = false;
            inflight = 0;
        }
        List<Job> dropped = new ArrayList<>();
        for (Job job : jobs.values()) {
            synchronized (job) {
                if (job.state == JobState.JOB_STATE_RUNNING) {
                    dropped.add(job);
                    for (Task t : job.tasks) {
                        t.cancelReason = "STEP_DOWN";
                        if (t.ctx != null) {
                            t.ctx.cancel(null);
                        }
                    }
                }
            }
        }
        for (Job job : dropped) {
            jobs.remove(job.id);
        }
        log("no longer coordinator (" + why + ")" + (dropped.isEmpty() ? "" : "; dropped " + dropped.size() + " unfinished job(s)"));
    }

    // ------------------------------------------------------------------ submit

    public SubmitJobReply submit(SubmitJobRequest r) {
        final String id = r.getJobId();
        if (id.isBlank()) {
            return reply(SubmitStatus.SUBMIT_STATUS_INVALID, "job_id is required", id);
        }
        synchronized (lock) {
            if (!active || !worker.isCoordinator()) {
                return reply(SubmitStatus.SUBMIT_STATUS_NOT_COORDINATOR, "this worker is not the coordinator", id);
            }
            if (jobs.containsKey(id)) {
                return reply(SubmitStatus.SUBMIT_STATUS_ALREADY_KNOWN, "job already known - just watch it", id);
            }
        }
        String problem = validate(r);
        if (problem != null) {
            return reply(SubmitStatus.SUBMIT_STATUS_INVALID, problem, id);
        }

        List<NodeInfo> workers = liveWorkers(); // a network call, so done outside the lock
        if (workers.isEmpty()) {
            return reply(SubmitStatus.SUBMIT_STATUS_NO_WORKERS, "no live workers", id);
        }

        final Job job;
        synchronized (lock) {
            if (!active || !worker.isCoordinator()) {
                return reply(SubmitStatus.SUBMIT_STATUS_NOT_COORDINATOR, "this worker is not the coordinator", id);
            }
            if (jobs.containsKey(id)) {
                return reply(SubmitStatus.SUBMIT_STATUS_ALREADY_KNOWN, "job already known - just watch it", id);
            }
            if (jobsThisTerm >= JOBS_PER_TERM) {
                return reply(SubmitStatus.SUBMIT_STATUS_TERM_EXHAUSTED,
                        "this term's " + JOBS_PER_TERM + " jobs are used up - a new coordinator is being elected", id);
            }
            // The slot is reserved HERE, atomically, at acceptance - not when the job finishes.
            jobsThisTerm++;
            inflight++;
            if (jobsThisTerm >= JOBS_PER_TERM) {
                termFull = true;
            }
            job = buildJob(r, workers);
            jobs.put(id, job);
            log("job " + shortId(id) + " " + label(job.type) + " accepted: " + job.tasks.size() + " task(s) over workers "
                    + workers.stream().map(NodeInfo::getId).toList() + " (job " + jobsThisTerm + " of " + JOBS_PER_TERM
                    + " this term)");
        }
        for (int i = 0; i < job.tasks.size(); i++) {
            final Task task = job.tasks.get(i);
            final NodeInfo first = workers.get(i % workers.size());
            taskPool.submit(() -> runTask(job, task, first));
        }
        return reply(SubmitStatus.SUBMIT_STATUS_ACCEPTED, "accepted", id);
    }

    private static String validate(SubmitJobRequest r) {
        switch (r.getType()) {
            case JOB_TYPE_MAX:
            case JOB_TYPE_PRIMECOUNT:
                return r.getNumbersCount() == 0 ? "numbers must not be empty" : null;
            case JOB_TYPE_PRIMESUM:
                return r.getRangeEnd() < r.getRangeStart() ? "range_end must not be smaller than range_start" : null;
            default:
                return "unknown job type";
        }
    }

    /** Divide the work as evenly as possible: one task per worker, sizes differ by at most one item. */
    private Job buildJob(SubmitJobRequest r, List<NodeInfo> workers) {
        final boolean isRange = r.getType() == JobType.JOB_TYPE_PRIMESUM;
        int[] numbers = null;
        if (!isRange) {
            numbers = new int[r.getNumbersCount()];
            for (int i = 0; i < numbers.length; i++) {
                numbers[i] = r.getNumbers(i);
            }
        }
        long total = isRange ? (long) r.getRangeEnd() - r.getRangeStart() + 1 : numbers.length;
        Job job = new Job(r.getJobId(), r.getType(), numbers, r.getRangeStart(), r.getRangeEnd(), total, term);

        int n = (int) Math.min(workers.size(), total);
        long base = total / n;
        long extra = total % n;
        long cursor = 0;
        for (int k = 0; k < n; k++) {
            long count = base + (k < extra ? 1 : 0);
            long first = isRange ? r.getRangeStart() + cursor : cursor;
            job.tasks.add(new Task(job.id + "-t" + (k + 1), first, count));
            cursor += count;
        }
        return job;
    }

    // ------------------------------------------------------------------ running one task (with recovery)

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
                        failJob(job, "no worker was available to run task " + task.id);
                        return;
                    }
                    sleepQuietly(1000);
                    continue;
                }
                waitingSince = 0;
            }

            final TaskRequest request;
            synchronized (job) {
                task.attempt++;
                task.target = target;
                task.state = TaskState.TASK_STATE_RUNNING;
                task.cancelReason = null;
                request = buildRequest(job, task);
                if (task.attempt > 1) {
                    job.message = "Task " + shortTask(task) + " reassigned to Worker " + target.getId()
                            + ", resuming at " + percent(task.processed, task.count) + "% from its checkpoint";
                }
            }
            if (target.getId() != selfId()) {
                worker.incrementJac(1); // JAC = tasks the coordinator hands to ANOTHER worker
            }
            log("task " + task.id + " -> Worker " + target.getId() + " (attempt " + request.getAttempt()
                    + (request.getResumeOffset() > 0 ? ", resuming after " + request.getResumeOffset() + " items" : "") + ")");

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
                        failJob(job, "task " + task.id + " failed " + errors + " times");
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

    /** Runs the task on {@code target} and follows its progress stream until the final result (or a failure). */
    private Outcome stream(Job job, Task task, NodeInfo target, TaskRequest request) {
        Context.CancellableContext ctx = Context.current().withCancellation();
        synchronized (job) {
            task.ctx = ctx;
        }
        try {
            return ctx.call(() -> {
                TaskServiceGrpc.TaskServiceBlockingStub stub =
                        TaskServiceGrpc.newBlockingStub(worker.connections().channel(target));
                Iterator<TaskUpdate> updates = stub.executeTask(request);
                while (updates.hasNext()) {
                    TaskUpdate u = updates.next();
                    applyUpdate(job, task, u);
                    if (u.getFinished()) {
                        return Outcome.DONE;
                    }
                }
                return Outcome.WORKER_FAILED; // the stream ended without the final result
            });
        } catch (StatusRuntimeException e) {
            return classify(job, task, e);
        } catch (Exception e) {
            return Outcome.WORKER_FAILED;
        } finally {
            ctx.cancel(null);
            synchronized (job) {
                task.ctx = null;
            }
        }
    }

    private Outcome classify(Job job, Task task, StatusRuntimeException e) {
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

    private void applyUpdate(Job job, Task task, TaskUpdate u) {
        synchronized (job) {
            if (job.state != JobState.JOB_STATE_RUNNING) {
                return;
            }
            if (u.getProcessed() >= task.processed) {
                task.processed = u.getProcessed();
                task.partial = u.getPartialValue();
            }
            task.state = TaskState.TASK_STATE_RUNNING;
            if (u.getFinished()) {
                task.state = TaskState.TASK_STATE_COMPLETED;
                task.processed = task.count;
                task.result = u.getPartialValue();
            }
        }
    }

    /** A worker died or stopped answering: queue its task again; it will resume from the last checkpoint. */
    private void workerFailed(Job job, Task task, NodeInfo dead) {
        int pct;
        synchronized (job) {
            task.state = TaskState.TASK_STATE_QUEUED;
            pct = percent(task.processed, task.count);
            job.message = "Worker " + dead.getId() + " failed - task " + shortTask(task) + " was at " + pct
                    + "%, reassigning it";
        }
        suspectUntil.put(dead.getId(), System.currentTimeMillis() + SUSPECT_MS);
        worker.connections().forget(dead);
        log("Worker " + dead.getId() + " FAILED - task " + task.id + " was at " + pct + "%, reassigning");
        taskPool.submit(() -> {
            try {
                worker.bootstrapStub().withDeadlineAfter(3, TimeUnit.SECONDS).reportFailure(FailureReport.newBuilder()
                        .setReporterId(selfId()).setSuspectId(dead.getId()).setReason("task stream failed").build());
            } catch (RuntimeException ignored) {
                // Bootstrap's own health checks will notice
            }
        });
    }

    private TaskRequest buildRequest(Job job, Task task) { // call while holding synchronized(job)
        TaskRequest.Builder b = TaskRequest.newBuilder()
                .setJobId(job.id).setTaskId(task.id).setType(job.type)
                .setTerm(job.term).setAttempt(task.attempt)
                .setResumeOffset(task.processed).setResumeValue(task.partial);
        if (job.type == JobType.JOB_TYPE_PRIMESUM) {
            b.setRangeStart((int) task.first).setRangeEnd((int) (task.first + task.count - 1));
        } else {
            for (long i = task.first; i < task.first + task.count; i++) {
                b.addNumbers(job.numbers[(int) i]);
            }
        }
        return b.build();
    }

    // ------------------------------------------------------------------ finishing a job

    private void checkCompletion(Job job) {
        boolean finished = false;
        synchronized (job) {
            if (job.state != JobState.JOB_STATE_RUNNING) {
                return;
            }
            long result = job.type == JobType.JOB_TYPE_MAX ? Long.MIN_VALUE : 0;
            for (Task t : job.tasks) {
                if (t.state != TaskState.TASK_STATE_COMPLETED) {
                    return;
                }
                result = job.type == JobType.JOB_TYPE_MAX ? Math.max(result, t.result) : result + t.result;
            }
            job.result = result;
            job.state = JobState.JOB_STATE_COMPLETED;
            job.doneAt = System.currentTimeMillis();
            job.message = "completed";
            finished = true;
        }
        if (finished) {
            log("job " + shortId(job.id) + " COMPLETED: " + label(job.type) + " = " + job.result);
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
            for (Task t : job.tasks) {
                t.cancelReason = "JOB_FAILED";
                if (t.ctx != null) {
                    t.ctx.cancel(null);
                }
            }
        }
        log("job " + shortId(job.id) + " FAILED: " + why);
        onJobFinished();
    }

    /** When the term's last job has finished, step down by starting the next election. */
    private void onJobFinished() {
        boolean startElection = false;
        synchronized (lock) {
            inflight = Math.max(0, inflight - 1);
            if (termFull && inflight == 0 && active && !electionRequested) {
                electionRequested = true;
                startElection = true;
            }
        }
        pruneFinishedJobs();
        if (startElection) {
            log("all " + JOBS_PER_TERM + " jobs of this term are done - ending the term and starting a new election");
            taskPool.submit(() -> worker.startElection("term limit reached after " + JOBS_PER_TERM + " jobs"));
        }
    }

    private void pruneFinishedJobs() {
        if (jobs.size() <= KEEP_FINISHED_JOBS) {
            return;
        }
        jobs.values().stream()
                .filter(j -> { synchronized (j) { return j.state != JobState.JOB_STATE_RUNNING; } })
                .sorted(Comparator.comparingLong(j -> { synchronized (j) { return j.doneAt; } }))
                .limit(jobs.size() - KEEP_FINISHED_JOBS)
                .forEach(j -> jobs.remove(j.id));
    }

    // ------------------------------------------------------------------ progress for clients

    /** The job's real progress right now, or null if this coordinator does not know the job. */
    public JobProgress snapshot(String jobId) {
        Job job = jobs.get(jobId);
        if (job == null) {
            return null;
        }
        synchronized (job) {
            long done = 0;
            JobProgress.Builder b = JobProgress.newBuilder();
            for (Task t : job.tasks) {
                long processed = t.state == TaskState.TASK_STATE_COMPLETED ? t.count : t.processed;
                done += processed;
                b.addTasks(TaskProgress.newBuilder()
                        .setTaskId(t.id)
                        .setWorkerId(t.state == TaskState.TASK_STATE_QUEUED || t.target == null ? 0 : t.target.getId())
                        .setState(t.state).setProcessed(processed).setTotal(t.count)
                        .setPercentComplete(round1(processed * 100.0 / t.count))
                        .setAttempt(t.attempt));
            }
            b.setJobId(job.id).setState(job.state)
                    .setPercentComplete(job.state == JobState.JOB_STATE_COMPLETED ? 100.0 : round1(done * 100.0 / job.total))
                    .setProcessedItems(done).setTotalItems(job.total)
                    .setCoordinatorId(selfId()).setTerm(job.term).setMessage(job.message);
            if (job.state == JobState.JOB_STATE_COMPLETED) {
                b.setHasResult(true).setResultValue(job.result).setResultText(label(job.type) + " = " + job.result);
            }
            return b.build();
        }
    }

    // ------------------------------------------------------------------ heartbeats to workers that run tasks

    private void watchdogTick() {
        Map<Integer, NodeInfo> busy = new HashMap<>();
        for (Job job : jobs.values()) {
            synchronized (job) {
                if (job.state != JobState.JOB_STATE_RUNNING) {
                    continue;
                }
                for (Task t : job.tasks) {
                    if (t.state == TaskState.TASK_STATE_RUNNING && t.target != null && t.target.getId() != selfId()) {
                        busy.put(t.target.getId(), t.target);
                    }
                }
            }
        }
        missedPings.keySet().retainAll(busy.keySet());
        for (NodeInfo node : busy.values()) {
            boolean alive;
            try {
                worker.connections().peer(node).withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                        .ping(PingRequest.newBuilder().setSenderId(selfId()).build());
                alive = true;
            } catch (StatusRuntimeException e) {
                alive = false;
            }
            if (alive) {
                missedPings.remove(node.getId());
                continue;
            }
            int missed = missedPings.merge(node.getId(), 1, Integer::sum);
            log("heartbeat to Worker " + node.getId() + " missed (" + missed + "/" + MAX_MISSED_PINGS + ")");
            if (missed >= MAX_MISSED_PINGS) {
                missedPings.remove(node.getId());
                cancelTasksOn(node.getId());
            }
        }
    }

    /** The worker stopped answering heartbeats although its stream is still open: cut the stream, which triggers reassignment. */
    private void cancelTasksOn(int workerId) {
        for (Job job : jobs.values()) {
            synchronized (job) {
                for (Task t : job.tasks) {
                    if (t.state == TaskState.TASK_STATE_RUNNING && t.target != null && t.target.getId() == workerId) {
                        t.cancelReason = "WORKER_DEAD";
                        if (t.ctx != null) {
                            t.ctx.cancel(new RuntimeException("worker " + workerId + " stopped answering heartbeats"));
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ choosing workers

    /** Live workers according to Bootstrap (or, if Bootstrap is unreachable, this worker plus its neighbours). */
    private List<NodeInfo> liveWorkers() {
        List<NodeInfo> list;
        try {
            list = new ArrayList<>(worker.bootstrapStub().withDeadlineAfter(2, TimeUnit.SECONDS)
                    .getWorkers(Empty.getDefaultInstance()).getWorkersList());
        } catch (StatusRuntimeException e) {
            log("Bootstrap unreachable (" + e.getStatus().getCode() + ") - using my neighbours instead");
            list = new ArrayList<>(worker.neighborList());
        }
        final NodeInfo self = worker.getSelf();
        if (list.stream().noneMatch(n -> n.getId() == self.getId())) {
            list.add(self);
        }
        final long now = System.currentTimeMillis();
        list.removeIf(n -> n.getId() != self.getId() && suspectUntil.getOrDefault(n.getId(), 0L) > now);
        list.sort(Comparator.comparingInt(NodeInfo::getId));
        return list;
    }

    /** The live worker that is running the fewest tasks right now. */
    private NodeInfo pickReplacement() {
        List<NodeInfo> candidates = liveWorkers();
        NodeInfo best = null;
        int bestLoad = Integer.MAX_VALUE;
        for (NodeInfo n : candidates) {
            int load = runningOn(n.getId());
            if (load < bestLoad) {
                best = n;
                bestLoad = load;
            }
        }
        return best;
    }

    private int runningOn(int workerId) {
        int count = 0;
        for (Job job : jobs.values()) {
            synchronized (job) {
                for (Task t : job.tasks) {
                    if (t.state == TaskState.TASK_STATE_RUNNING && t.target != null && t.target.getId() == workerId) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    // ------------------------------------------------------------------ small helpers

    private boolean isActive() {
        synchronized (lock) {
            return active;
        }
    }

    private SubmitJobReply reply(SubmitStatus status, String message, String jobId) {
        CoordinatorInfo hint = worker.getCoordinator();
        return SubmitJobReply.newBuilder().setStatus(status).setMessage(message).setJobId(jobId)
                .setCoordinatorHint(hint).build();
    }

    private int selfId() {
        NodeInfo s = worker.getSelf();
        return s == null ? 0 : s.getId();
    }

    private static int percent(long processed, long total) {
        return total == 0 ? 100 : (int) (processed * 100 / total);
    }

    private static double round1(double v) {
        return Math.round(v * 10) / 10.0;
    }

    private static String label(JobType type) {
        return type.name().replace("JOB_TYPE_", "");
    }

    private static String shortId(String id) {
        return id.length() > 8 ? id.substring(0, 8) : id;
    }

    private static String shortTask(Task t) {
        int dash = t.id.lastIndexOf('-');
        return dash >= 0 ? t.id.substring(dash + 1) : t.id;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(String message) {
        System.out.println("[Worker " + selfId() + " | COORDINATOR] " + message);
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
