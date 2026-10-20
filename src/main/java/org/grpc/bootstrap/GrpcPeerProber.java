package org.grpc.bootstrap;

import ds.grpc.NodeInfo;
import ds.grpc.PeerServiceGrpc;
import ds.grpc.PingRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Liveness probe: a PeerService.Ping with a short deadline. */
public class GrpcPeerProber implements WorkerRegistry.Prober {

    private static final long PING_DEADLINE_MS = 500;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    @Override
    public boolean isAlive(NodeInfo node) {
        try {
            ManagedChannel channel = channels.computeIfAbsent(key(node), k ->
                    ManagedChannelBuilder.forAddress(node.getHost(), node.getPort()).usePlaintext().build());
            PeerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(PING_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .ping(PingRequest.newBuilder().setSenderId(0).build());
            return true;
        } catch (StatusRuntimeException e) {
            // A channel to a dead server sits in exponential reconnect back-off, which would make a
            // worker that restarts on the same port look dead for a long time. Start fresh next time.
            forget(node);
            return false;
        }
    }

    @Override
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
