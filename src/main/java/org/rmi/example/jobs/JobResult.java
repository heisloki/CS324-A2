package org.rmi.example.jobs; // Package for job-related classes

import java.io.Serializable; // Results must also be serializable for RMI

// Represents the result of a job executed by a worker (or the combined
// result of a full client-submitted job, once the coordinator merges the
// partial results from each worker).
public class JobResult implements Serializable {
    private final String result; // Human-readable description, for logging/display
    private final long value;    // The actual computed number (max / sum / count), used for aggregation

    // Kept for backward compatibility with existing call sites that only
    // pass a description (defaults value to 0).
    public JobResult(String result) {
        this(result, 0L);
    }

    // Preferred constructor: carries both a description and the numeric result.
    public JobResult(String result, long value) {
        this.result = result;
        this.value = value;
    }

    // Getter for result description
    public String getResult() { return result; }

    // Getter for the numeric result (used when combining partial results)
    public long getValue() { return value; }
}
