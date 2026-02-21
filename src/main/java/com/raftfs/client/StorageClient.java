package com.raftfs.client;

import com.raftfs.proto.*;
import com.raftfs.ring.ConsistentHashRing;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StorageClient {

    private static final Logger log = LoggerFactory.getLogger(StorageClient.class);
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 100;

    private final ConsistentHashRing ring;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public StorageClient(List<String> nodeAddresses) {
        this.ring = new ConsistentHashRing();
        nodeAddresses.forEach(ring::addNode);
    }

    public boolean put(String key, byte[] data) {
        String target = ring.getNode(key);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                var stub = getStub(target);
                PutObjectResponse response = stub.putObject(PutObjectRequest.newBuilder()
                        .setKey(key)
                        .setData(com.google.protobuf.ByteString.copyFrom(data))
                        .build());
                return response.getSuccess();
            } catch (Exception e) {
                log.warn("Put attempt {} failed for key {}: {}", attempt + 1, key, e.getMessage());
                target = getNextReplica(key, attempt + 1);
                sleep(RETRY_DELAY_MS * (attempt + 1));
            }
        }
        return false;
    }

    public byte[] get(String key) {
        String target = ring.getNode(key);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                var stub = getStub(target);
                GetObjectResponse response = stub.getObject(GetObjectRequest.newBuilder()
                        .setKey(key)
                        .build());
                if (response.getFound()) {
                    return response.getData().toByteArray();
                }
                return null;
            } catch (Exception e) {
                log.warn("Get attempt {} failed for key {}: {}", attempt + 1, key, e.getMessage());
                target = getNextReplica(key, attempt + 1);
                sleep(RETRY_DELAY_MS * (attempt + 1));
            }
        }
        return null;
    }

    public boolean delete(String key) {
        String target = ring.getNode(key);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                var stub = getStub(target);
                DeleteObjectResponse response = stub.deleteObject(DeleteObjectRequest.newBuilder()
                        .setKey(key)
                        .build());
                return response.getSuccess();
            } catch (Exception e) {
                log.warn("Delete attempt {} failed for key {}: {}", attempt + 1, key, e.getMessage());
                target = getNextReplica(key, attempt + 1);
                sleep(RETRY_DELAY_MS * (attempt + 1));
            }
        }
        return false;
    }

    public void addNode(String nodeAddress) {
        ring.addNode(nodeAddress);
    }

    public void removeNode(String nodeAddress) {
        ring.removeNode(nodeAddress);
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }

    private String getNextReplica(String key, int offset) {
        List<String> replicas = ring.getReplicaNodes(key, MAX_RETRIES);
        return offset < replicas.size() ? replicas.get(offset) : replicas.get(0);
    }

    private ObjectStorageServiceGrpc.ObjectStorageServiceBlockingStub getStub(String target) {
        ManagedChannel channel = channels.computeIfAbsent(target, addr ->
                ManagedChannelBuilder.forTarget(addr).usePlaintext().build());
        return ObjectStorageServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
