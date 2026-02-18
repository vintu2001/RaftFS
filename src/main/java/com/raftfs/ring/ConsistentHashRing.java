package com.raftfs.ring;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentHashRing {

    private static final int DEFAULT_VIRTUAL_NODES = 150;

    private final int virtualNodes;
    private final TreeMap<Integer, String> ring = new TreeMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public void addNode(String nodeAddress) {
        rwLock.writeLock().lock();
        try {
            for (int i = 0; i < virtualNodes; i++) {
                int hash = murmur3Hash(nodeAddress + "#" + i);
                ring.put(hash, nodeAddress);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeNode(String nodeAddress) {
        rwLock.writeLock().lock();
        try {
            for (int i = 0; i < virtualNodes; i++) {
                int hash = murmur3Hash(nodeAddress + "#" + i);
                ring.remove(hash);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String getNode(String objectKey) {
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) return null;
            int hash = murmur3Hash(objectKey);
            var entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            return entry.getValue();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getReplicaNodes(String objectKey, int replicationFactor) {
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) return Collections.emptyList();

            int hash = murmur3Hash(objectKey);
            Set<String> seen = new LinkedHashSet<>();
            NavigableMap<Integer, String> tailMap = ring.tailMap(hash, true);

            for (String node : tailMap.values()) {
                if (seen.size() >= replicationFactor) break;
                seen.add(node);
            }
            if (seen.size() < replicationFactor) {
                for (String node : ring.values()) {
                    if (seen.size() >= replicationFactor) break;
                    seen.add(node);
                }
            }
            return new ArrayList<>(seen);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int nodeCount() {
        rwLock.readLock().lock();
        try {
            return new HashSet<>(ring.values()).size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private static int murmur3Hash(String key) {
        return Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt();
    }
}
