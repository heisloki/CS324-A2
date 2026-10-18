package org.rmi.example.bootstrap; // Package for bootstrap-related classes

import org.rmi.example.worker.WorkerInterface; // Workers register here
import java.rmi.Remote;                     // All RMI interfaces must extend Remote
import java.rmi.RemoteException;            // RMI methods must declare this exception
import java.util.List;                      // To return a list of workers

// Remote interface that defines what the bootstrap node can do
public interface BootstrapService extends Remote {
    // Register a worker with the bootstrap node
    void registerWorker(WorkerInterface worker) throws RemoteException;

    // Explicitly removes a worker
    void unregisterWorker(WorkerInterface worker) throws RemoteException;

    // Return the list of currently registered, REACHABLE workers.
    List<WorkerInterface> getWorkers() throws RemoteException;

    // ================= coordinator tracking =================

    // Called by a worker once it has won an election
    void updateCoordinator(WorkerInterface coordinator, int coordinatorId, int coordinatorJac) throws RemoteException;

    // Returns the current coordinator's live stub, or null if none has been
    // elected yet
    WorkerInterface getCoordinator() throws RemoteException;
}
