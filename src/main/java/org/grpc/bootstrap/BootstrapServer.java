package org.grpc.bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * The Bootstrap Node process.   Usage: BootstrapServer [port]      (default 50050)
 * Run it first; workers and clients find everything else through it.
 */
public class BootstrapServer {

    public static final int DEFAULT_PORT = 50050;
    private static final long HEALTH_CHECK_INTERVAL_MS = 2000;

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        GrpcPeerProber prober = new GrpcPeerProber();
        WorkerRegistry registry = new WorkerRegistry(prober);

        Server server = ServerBuilder.forPort(port)
                .addService(new BootstrapGrpcService(registry))
                .build()
                .start();

        registry.startHealthChecks(HEALTH_CHECK_INTERVAL_MS);
        System.out.println("Bootstrap Node (gRPC) running on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            registry.stop();
            prober.shutdown();
            server.shutdown();
        }));

        server.awaitTermination();
    }
}
