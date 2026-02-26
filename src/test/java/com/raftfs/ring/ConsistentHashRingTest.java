package com.raftfs.ring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    private ConsistentHashRing ring;

    @BeforeEach
    void setUp() {
        ring = new ConsistentHashRing();
    }

    @Test
    void emptyRingReturnsNull() {
        assertNull(ring.getNode("any-key"));
    }

    @Test
    void singleNodeHandlesAllKeys() {
        ring.addNode("node-0:8080");

        assertEquals("node-0:8080", ring.getNode("key1"));
        assertEquals("node-0:8080", ring.getNode("key2"));
        assertEquals("node-0:8080", ring.getNode("key3"));
    }

    @Test
    void nodeCountReflectsAddedNodes() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");
        ring.addNode("node-2:8080");

        assertEquals(3, ring.nodeCount());
    }

    @Test
    void removeNodeDecreasesCount() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");
        ring.removeNode("node-0:8080");

        assertEquals(1, ring.nodeCount());
    }

    @Test
    void getReplicaNodesReturnsDistinctNodes() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");
        ring.addNode("node-2:8080");

        List<String> replicas = ring.getReplicaNodes("test-key", 3);
        assertEquals(3, replicas.size());
        assertEquals(3, new HashSet<>(replicas).size());
    }

    @Test
    void getReplicaNodesWithInsufficientNodes() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");

        List<String> replicas = ring.getReplicaNodes("test-key", 3);
        assertEquals(2, replicas.size());
    }

    @Test
    void consistentRoutingForSameKey() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");
        ring.addNode("node-2:8080");

        String target1 = ring.getNode("my-object");
        String target2 = ring.getNode("my-object");
        assertEquals(target1, target2);
    }

    @Test
    void loadDistributionIsReasonablyBalanced() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");
        ring.addNode("node-2:8080");

        Map<String, Integer> distribution = new HashMap<>();
        int totalKeys = 10000;

        for (int i = 0; i < totalKeys; i++) {
            String node = ring.getNode("key-" + i);
            distribution.merge(node, 1, Integer::sum);
        }

        for (int count : distribution.values()) {
            double ratio = (double) count / totalKeys;
            assertTrue(ratio > 0.15 && ratio < 0.55,
                    "Node got " + ratio + " of keys, expected ~0.33");
        }
    }

    @Test
    void minimalKeyMigrationOnNodeAddition() {
        ring.addNode("node-0:8080");
        ring.addNode("node-1:8080");

        Map<String, String> beforeMapping = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            beforeMapping.put(key, ring.getNode(key));
        }

        ring.addNode("node-2:8080");

        int migrated = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            if (!ring.getNode(key).equals(beforeMapping.get(key))) {
                migrated++;
            }
        }

        double migrationRatio = (double) migrated / 1000;
        assertTrue(migrationRatio < 0.60, "Too many keys migrated: " + migrationRatio);
    }
}
