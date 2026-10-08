package org.example.worker; // Package for worker-related classes

import org.example.jobs.Job;       // Import the Job data structure (task details)
import org.example.jobs.JobResult; // Import the JobResult structure (task output)
import java.rmi.Remote;            // All RMI interfaces must extend Remote
import java.rmi.RemoteException;   // RMI methods must declare this exception

// Remote interface that defines what workers can do
// This is what the leader and bootstrap node will call remotely
public interface WorkerInterface extends Remote {
    // Return the worker's unique ID (used in elections and logging)
    int getId() throws RemoteException;

    // Return the worker's Job Allocation Counter (used in leader election fairness)
    int getJobAllocationCounter() throws RemoteException;

    // Execute a job sent by the leader and return the result
    JobResult executeJob(Job job) throws RemoteException;

    // ================= leader election =================

    void startElection() throws RemoteException;

    void onElectionMessage(String messageId, int candidateId, int candidateJac) throws RemoteException;

    void onCoordinatorMessage(String messageId, WorkerInterface coordinatorStub,
                              int coordinatorId, int coordinatorJac) throws RemoteException;

    // Links this worker to another worker in the unstructured network.
    void addNeighbor(WorkerInterface neighbor) throws RemoteException;

    // ================= Added for job distribution =================

    // Client-facing entry point.
    JobResult submitJob(Job job) throws RemoteException;
}
