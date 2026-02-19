package com.raftfs.storage;

import com.raftfs.client.StorageGrpcClient;
import com.raftfs.ring.ConsistentHashRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int REPLICATION_FACTOR = 3;
    private static final int QUORUM = 2;
    private static final int REPLICATION_TIMEOUT_MS = 5000;

    private final ConsistentHashRing ring;
    private final ChunkStore localStore;
    private final String localAddress;
    private final ConcurrentHashMap<String, StorageGrpcClient> clients = new ConcurrentHashMap<>();
    private final ExecutorService replicationExecutor = Executors.newFixedThreadPool(16);

    public ReplicationManager(ConsistentHashRing ring, ChunkStore localStore, String localAddress) {
        this.ring = ring;
        this.localStore = localStore;
        this.localAddress = localAddress;
    }

    public boolean putObject(String key, byte[] data) throws Exception {
        List<String> replicaNodes = ring.getReplicaNodes(key, REPLICATION_FACTOR);
        if (replicaNodes.size() < QUORUM) {
            throw new IllegalStateException(
                    "Insufficient nodes for quorum: need " + QUORUM + ", available " + replicaNodes.size());
        }

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch quorumLatch = new CountDownLatch(QUORUM);

        for (String nodeAddr : replicaNodes) {
            replicationExecutor.submit(() -> {
                try {
                    boolean written = nodeAddr.equals(localAddress)
                            ? localStore.write(key, data)
                            : getClient(nodeAddr).writeChunk(key, data);

                    if (written) {
                        successCount.incrementAndGet();
                        quorumLatch.countDown();
                    }
                } catch (Exception e) {
                    log.warn("Replication to {} failed for key {}: {}", nodeAddr, key, e.getMessage());
                }
            });
        }

        boolean quorumMet = quorumLatch.await(REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (!quorumMet || successCount.get() < QUORUM) {
            log.error("Quorum write failed for key {}: {}/{} acknowledged",
                    key, successCount.get(), REPLICATION_FACTOR);
            cleanupPartialWrites(key, replicaNodes);
            return false;
        }

        log.info("Quorum write succeeded for key {}: {}/{} nodes",
                key, successCount.get(), REPLICATION_FACTOR);
        return true;
    }

    public Optional<byte[]> getObject(String key) {
        List<String> nodes = ring.getReplicaNodes(key, REPLICATION_FACTOR);

        for (String nodeAddr : nodes) {
            try {
                byte[] data = nodeAddr.equals(localAddress)
                        ? localStore.read(key)
                        : getClient(nodeAddr).readChunk(key);
                if (data != null) return Optional.of(data);
            } catch (Exception e) {
                log.warn("Read from {} failed for key {}, trying next replica", nodeAddr, key);
            }
        }
        return Optional.empty();
    }

    public boolean deleteObject(String key) {
        List<String> nodes = ring.getReplicaNodes(key, REPLICATION_FACTOR);
        AtomicInteger deleted = new AtomicInteger(0);

        for (String nodeAddr : nodes) {
            try {
                boolean success = nodeAddr.equals(localAddress)
                        ? localStore.delete(key)
                        : getClient(nodeAddr).deleteChunk(key);
                if (success) deleted.incrementAndGet();
            } catch (Exception e) {
                log.warn("Delete from {} failed for key {}", nodeAddr, key);
            }
        }
        return deleted.get() >= QUORUM;
    }

    private void cleanupPartialWrites(String key, List<String> nodes) {
        replicationExecutor.submit(() -> {
            for (String node : nodes) {
                try {
                    if (node.equals(localAddress)) {
                        localStore.delete(key);
                    } else {
                        getClient(node).deleteChunk(key);
                    }
                } catch (Exception ignored) {
                }
            }
        });
    }

    private StorageGrpcClient getClient(String nodeAddr) {
        return clients.computeIfAbsent(nodeAddr, StorageGrpcClient::new);
    }

    public void shutdown() {
        replicationExecutor.shutdownNow();
        clients.values().forEach(StorageGrpcClient::shutdown);
    }
}
