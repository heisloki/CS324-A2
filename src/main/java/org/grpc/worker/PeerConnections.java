package org.grpc.worker;

import ds.grpc.NodeInfo;
import ds.grpc.PeerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One cached gRPC channel per remote worker (the A2 replacement for holding an RMI stub).
 *
 * Opening a channel is expensive, so we open it once and reuse it. When a call to a node fails we
 * call {@link #forget}: a channel to a dead server sits in exponential reconnect back-off, which
 * would make a worker that restarts on the same port look dead for a long time.
 *
 * Later steps (TaskService, ReplicaService, ...) reuse {@link #channel} to build their own stubs.
 */
public class PeerConnections {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ManagedChannel channel(NodeInfo node) {
        return channels.computeIfAbsent(key(node), k ->
                ManagedChannelBuilder.forAddress(node.getHost(), node.getPort()).usePlaintext().build());
    }

    public PeerServiceGrpc.PeerServiceBlockingStub peer(NodeInfo node) {
        return PeerServiceGrpc.newBlockingStub(channel(node));
    }

    public void forget(NodeInfo node) {
        ManagedChannel channel = channels.remove(key(node));
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }

    private static String key(NodeInfo node) {
        return node.getHost() + ":" + node.getPort();
    }
}
