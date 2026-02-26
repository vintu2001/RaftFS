package com.raftfs.storage;

import com.raftfs.ring.ConsistentHashRing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationManagerTest {

    private ConsistentHashRing ring;
    private LocalChunkStore localStore;
    private ReplicationManager replicationManager;

    @BeforeEach
    void setUp() {
        ring = new ConsistentHashRing();
        ring.addNode("localhost:8080");
        localStore = new LocalChunkStore(
                System.getProperty("java.io.tmpdir") + "/raftfs-test-" + System.nanoTime());
        replicationManager = new ReplicationManager(ring, localStore, "localhost:8080");
    }

    @Test
    void putAndGetWithSingleNode() throws Exception {
        byte[] data = "hello world".getBytes();
        boolean written = replicationManager.putObject("key1", data);
        assertTrue(written);

        Optional<byte[]> result = replicationManager.getObject("key1");
        assertTrue(result.isPresent());
        assertArrayEquals(data, result.get());
    }

    @Test
    void getReturnsEmptyForMissingKey() {
        Optional<byte[]> result = replicationManager.getObject("nonexistent");
        assertTrue(result.isEmpty());
    }

    @Test
    void deleteRemovesObject() throws Exception {
        replicationManager.putObject("key1", "data".getBytes());
        boolean deleted = replicationManager.deleteObject("key1");
        assertTrue(deleted);

        Optional<byte[]> result = replicationManager.getObject("key1");
        assertTrue(result.isEmpty());
    }

    @Test
    void putFailsWithNoNodes() {
        ConsistentHashRing emptyRing = new ConsistentHashRing();
        ReplicationManager mgr = new ReplicationManager(emptyRing, localStore, "localhost:8080");

        assertThrows(IllegalStateException.class,
                () -> mgr.putObject("key1", "data".getBytes()));
    }
}
