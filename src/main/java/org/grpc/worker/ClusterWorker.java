
package org.grpc.worker;

import org.grpc.bootstrap.BootstrapServer;
import org.grpc.coordinator.JobEngine;
import org.grpc.coordinator.JobGrpcService;

public class ClusterWorker extends WorkerNode {

    private final JobEngine engine;
    private final ReplicaStore replicaStore;

    public ClusterWorker(
            String host,
            int port,
            int requestedId,
            String bootstrapHost,
            int bootstrapPort) {

        super(host, port, requestedId, bootstrapHost, bootstrapPort);

        // Create the replica store before the job engine.
        this.replicaStore = new ReplicaStore();

        // The engine uses the replica store to recover replicated state.
        this.engine = new JobEngine(this, replicaStore);

        addService(new TaskGrpcService(this));
        addService(new JobGrpcService(engine));
        addService(new ReplicaGrpcService(replicaStore));
    }

    @Override
    protected void onBecameCoordinator(long term) {
        engine.activate(term);
    }

    @Override
    protected void onLostCoordinatorRole() {
        engine.deactivate("A newer coordinator took over");
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println(
                    "Usage: ClusterWorker <port> [id] [myHost] "
                            + "[bootstrapHost] [bootstrapPort]");
            return;
        }

        int port = Integer.parseInt(args[0]);
        int id = args.length > 1
                ? Integer.parseInt(args[1])
                : 0;

        String myHost = args.length > 2
                ? args[2]
                : "localhost";

        String bootstrapHost = args.length > 3
                ? args[3]
                : "localhost";

        int bootstrapPort = args.length > 4
                ? Integer.parseInt(args[4])
                : BootstrapServer.DEFAULT_PORT;

        ClusterWorker worker = new ClusterWorker(
                myHost,
                port,
                id,
                bootstrapHost,
                bootstrapPort);

        worker.start();
        worker.awaitTermination();
    }
}
