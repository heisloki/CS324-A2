package org.rmi.example.jobs; // Package for job-related classes

import java.io.Serializable; // Jobs must be serializable to travel over RMI

// Represents a computational job submitted by a client
public class Job implements Serializable {
    private final String type; // Type of job (e.g., "MAX", "PRIMESUM", "PRIMECOUNT")
    private final Object data; // Input data (could be a list of numbers, or a range)

    // Constructor: initializes job type and data
    public Job(String type, Object data) {
        this.type = type;
        this.data = data;
    }

    // Getter for job type
    public String getType() { return type; }

    // Getter for job data
    public Object getData() { return data; }
}
