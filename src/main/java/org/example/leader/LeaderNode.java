package org.example.leader; // Package for leader/coordinator-related classes

import org.example.jobs.Job;
import org.example.jobs.JobResult;
import org.example.jobs.JobUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// This class is a stateless helper that the currently-elected WorkerNode
// calls into (see WorkerNode.submitJob) to:
//   1) split a client's job into roughly-even sub-jobs across active workers
//   2) combine the partial JobResults it gets back into one final JobResult
public final class LeaderNode {

    private LeaderNode() {} // static-only utility class, no instances

    // Splits `job` into up to `numWorkers` roughly-even sub-jobs.
    public static List<Job> splitJob(Job job, int numWorkers) {
        numWorkers = Math.max(1, numWorkers);
        switch (job.getType()) {
            case "MAX":
            case "PRIMECOUNT":
                return splitByChunks(job, numWorkers);
            case "PRIMESUM":
                return splitByRange(job, numWorkers);
            default:
                throw new IllegalArgumentException("Unknown job type: " + job.getType());
        }
    }

    // Splits a list of numbers into up to numWorkers contiguous chunks,
    // as even as possible (first `remainder` workers get one extra element).
    private static List<Job> splitByChunks(Job job, int numWorkers) {
        int[] all = JobUtils.toIntArray(job.getData());
        List<Job> subJobs = new ArrayList<>();

        int n = all.length;
        int base = n / numWorkers;
        int remainder = n % numWorkers;
        int index = 0;

        for (int w = 0; w < numWorkers && index < n; w++) {
            int chunkSize = base + (w < remainder ? 1 : 0);
            if (chunkSize == 0) continue;
            int[] chunk = Arrays.copyOfRange(all, index, index + chunkSize);
            subJobs.add(new Job(job.getType(), chunk));
            index += chunkSize;
        }
        return subJobs;
    }

    // Splits a numeric range [start, end] into up to numWorkers contiguous
    // sub-ranges, as even as possible. Example: PRIMESUM(1,1000) with 4
    // workers -> (1,250) (251,500) (501,750) (751,1000).
    private static List<Job> splitByRange(Job job, int numWorkers) {
        int[] range = JobUtils.toIntArray(job.getData());
        int start = range[0];
        int end = range[1];
        long total = (long) end - start + 1;

        List<Job> subJobs = new ArrayList<>();
        if (total <= 0) return subJobs;

        long base = total / numWorkers;
        long remainder = total % numWorkers;
        long cursor = start;

        for (int w = 0; w < numWorkers && cursor <= end; w++) {
            long size = base + (w < remainder ? 1 : 0);
            if (size <= 0) continue;
            long subStart = cursor;
            long subEnd = cursor + size - 1;
            subJobs.add(new Job(job.getType(), new int[]{(int) subStart, (int) subEnd}));
            cursor = subEnd + 1;
        }
        return subJobs;
    }

    // Combines partial JobResults (one per worker) into the single final
    // result the client sees.
    public static JobResult combineResults(String jobType, List<JobResult> partials) {
        switch (jobType) {
            case "MAX": {
                long max = Long.MIN_VALUE;
                for (JobResult r : partials) {
                    if (r.getValue() > max) max = r.getValue();
                }
                return new JobResult("MAX = " + max, max);
            }
            case "PRIMESUM": {
                long sum = 0;
                for (JobResult r : partials) sum += r.getValue();
                return new JobResult("PRIMESUM = " + sum, sum);
            }
            case "PRIMECOUNT": {
                long count = 0;
                for (JobResult r : partials) count += r.getValue();
                return new JobResult("PRIMECOUNT = " + count, count);
            }
            default:
                throw new IllegalArgumentException("Unknown job type: " + jobType);
        }
    }
}
