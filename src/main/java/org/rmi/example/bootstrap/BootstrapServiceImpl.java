package org.rmi.example.bootstrap;

import org.rmi.example.worker.WorkerInterface; // Worker interface reference
import java.rmi.RemoteException;           // RMI exception handling
import java.rmi.server.UnicastRemoteObject; // Base class for RMI objects
import java.util.ArrayList;                // Store workers in a list
import java.util.List;

// Implementation of the BootstrapService interface
public class BootstrapServiceImpl extends UnicastRemoteObject implements BootstrapService {
    // Internal list to keep track of registered workers
    private final List<WorkerInterface> workers;

    // Currently elected coordinator (null until the first election concludes)
    private volatile WorkerInterface coordinator;
    private volatile int coordinatorId = -1;
    private volatile int coordinatorJac = 0;

    // Constructor: initializes the list and exports object for RMI
    protected BootstrapServiceImpl() throws RemoteException {
        super(); // Calls UnicastRemoteObject constructor
        this.workers = new ArrayList<>();
    }

    // Register a new worker
    @Override
    public synchronized void registerWorker(WorkerInterface worker) throws RemoteException {
        workers.add(worker);
        System.out.println("Worker registered: ID = " + worker.getId());
    }

    // Explicitly remove a worker.
    @Override
    public synchronized void unregisterWorker(WorkerInterface worker) throws RemoteException {
        boolean removed = workers.remove(worker);
        if (removed) {
            String label;
            try {
                label = "Worker " + worker.getId();
            } catch (RemoteException e) {
                label = "A worker";
            }
            System.out.println(label + " unregistered (graceful shutdown).");
        }
    }

    // Return all registered, REACHABLE workers.
    @Override
    public List<WorkerInterface> getWorkers() throws RemoteException {
        List<WorkerInterface> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(workers);
        }

        List<WorkerInterface> alive = new ArrayList<>();
        for (WorkerInterface w : snapshot) {
            try {
                w.getId(); // cheap liveness probe
                alive.add(w);
            } catch (RemoteException e) {
                System.out.println("Bootstrap: pruning an unreachable worker from the list.");
            }
        }

        synchronized (this) {
            workers.retainAll(alive); // drop the dead ones from the real list too
        }
        return alive;
    }

    // Record who the current coordinator is (called by the worker that just won an election)
    @Override
    public synchronized void updateCoordinator(WorkerInterface coordinator, int coordinatorId, int coordinatorJac) {
        this.coordinator = coordinator;
        this.coordinatorId = coordinatorId;
        this.coordinatorJac = coordinatorJac;
        System.out.println("Bootstrap notified: new coordinator is Worker " + coordinatorId
                + " (JAC=" + coordinatorJac + ")");
    }

    // Return the current coordinator's live stub (null if none elected yet)
    @Override
    public synchronized WorkerInterface getCoordinator() {
        return coordinator;
    }
}