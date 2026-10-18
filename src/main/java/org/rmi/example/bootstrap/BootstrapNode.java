package org.rmi.example.bootstrap;

import java.rmi.registry.LocateRegistry; // To start RMI registry
import java.rmi.registry.Registry;       // Registry reference

// Main class to start the bootstrap node process
public class BootstrapNode {
    public static void main(String[] args) throws Exception {
        // Start RMI registry on port 1099
        Registry registry = LocateRegistry.createRegistry(1099);

        // Create and bind the bootstrap service implementation
        BootstrapServiceImpl service = new BootstrapServiceImpl();
        registry.bind("BootstrapService", service);

        // Confirmation message
        System.out.println("Bootstrap Node running on port 1099...");

        Thread.sleep(Long.MAX_VALUE);
    }
}
