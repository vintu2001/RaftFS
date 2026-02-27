# PROJECT 2: Distributed Object Storage & Container Service

**Stack: Java 17, Raft Consensus, Kubernetes, Docker**
**GitHub Repository**

---

## 1. What This System Is

A **distributed object storage system** similar to a simplified GFS or MinIO, rewritten in **Java 17**. Raft manages cluster membership and chunk placement decisions (master/leader). A consistent hashing ring handles client-side routing to the correct storage node without going through the leader for every read.

**Core Responsibilities:**
- **Object Storage** — Store and retrieve arbitrary binary objects (blobs) across a cluster
- **Chunk Placement (Raft)** — Consensus-based authoritative record of which node holds which chunk
- **Client Routing (Consistent Hashing)** — Client-side ring for O(1) node lookup without leader involvement
- **Quorum Replication (RF=3)** — Fault-tolerant writes requiring 2/3 acknowledgment
- **Container Orchestration** — Kubernetes StatefulSets for persistent storage lifecycle

**Key Architectural Boundary:**
- **Raft = Control Plane**: Ensures all nodes agree on the authoritative chunk map. All placement decisions go through the Raft leader and get replicated to followers.
- **Consistent Hashing = Data Plane Routing**: Client computes which node to contact for a given object key — no leader query needed for reads.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                             │
│              StorageClient SDK / CLI Tool                        │
│   ┌────────────────────────────────────────────────────────┐    │
│   │  ConsistentHashRing (local)                            │    │
│   │  hash(objectKey) → nodeAddress                         │    │
│   │  GetReplicaNodes(key, RF=3) → [node1, node2, node3]   │    │
│   └──────────────────────────┬─────────────────────────────┘    │
└──────────────────────────────┼──────────────────────────────────┘
                               │ gRPC
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE NODE (one of N)                       │
│                                                                 │
│  ┌──────────────────┐  ┌────────────────────┐                   │
│  │  gRPC Server     │  │  Chunk Store       │                   │
│  │  - PutObject     │  │  - read/write to   │                   │
│  │  - GetObject     │  │    local disk       │                   │
│  │  - DeleteObject  │  │  - checksums        │                   │
│  │  - WriteChunk    │  │  - file layout      │                   │
│  │    (replication)│  │    management       │                   │
│  └───────┬──────────┘  └────────────────────┘                   │
│          │ if chunk ownership query                              │
│          ▼                                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Raft Module                                             │   │
│  │  - RaftNode (leader election, log replication)           │   │
│  │  - ChunkPlacementFSM (applies committed log entries)    │   │
│  │  - Log persistence (WAL on disk)                        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

Cluster Topology (Raft):
┌────────────┐    ┌────────────┐    ┌────────────┐
│  Node 0    │    │  Node 1    │    │  Node 2    │
│  (Leader)  │◄──►│  (Follower)│◄──►│  (Follower)│
│            │    │            │    │            │
│  Raft log  │    │  Raft log  │    │  Raft log  │
│  Chunk FSM │    │  Chunk FSM │    │  Chunk FSM │
│  /data/    │    │  /data/    │    │  /data/    │
└────────────┘    └────────────┘    └────────────┘
```

---

## 3. Data Flow Diagrams

### 3.1 Write Path (PutObject — RF=3 Quorum Write)

```
Client                     StorageNode (Primary)        Replica1    Replica2
  │                              │                         │           │
  │── PutObject(key, data) ────>│                         │           │
  │                              │                         │           │
  │                 ConsistentHashRing.getReplicaNodes(key, 3)         │
  │                              │ = [self, replica1, replica2]        │
  │                              │                         │           │
  │                              │── write locally ───────>│           │
  │                              │── WriteChunk(key,data) ────────────>│
  │                              │── WriteChunk(key,data) ─────────────────>│
  │                              │                         │           │
  │                              │<── ACK ─────────────────│           │
  │                              │<── ACK ─────────────────────────────│
  │                              │                         │ (timeout) │
  │                              │                         │           │
  │                  quorumCount = 2 (self + replica1) >= 2 ✓          │
  │                              │                         │           │
  │                              │── Raft Propose ────────>│           │
  │                              │   {type: "place",       │           │
  │                              │    key, nodes=[0,1,2]}  │           │
  │                              │   (committed once       │           │
  │                              │    majority agrees)     │           │
  │                              │                         │           │
  │<── PutResponse(success) ───│                         │           │
```

### 3.2 Read Path (GetObject)

```
Client                     StorageNode (Target)
  │                              │
  │  ConsistentHashRing           │
  │  .getNode(key) → nodeAddr    │
  │                              │
  │── GetObject(key) ──────────>│
  │                              │── ChunkStore.read(key)
  │                              │   [check local disk]
  │                              │
  │                              │   [HIT] → return data
  │                              │   [MISS] → query Raft FSM
  │                              │           → get actual node list
  │                              │           → redirect client or
  │                              │             proxy read from correct node
  │                              │
  │<── GetResponse(data) ──────│
```

---

## 4. Directory Structure

```
distributed-object-storage/
├── pom.xml                           ← Maven build with Java 17
├── Dockerfile
├── README.md
├── src/main/java/com/storage/
│   ├── StorageNodeApplication.java   ← Spring Boot main class
│   ├── config/
│   │   ├── RaftConfig.java           ← Raft cluster configuration
│   │   └── StorageConfig.java        ← Data dir, port settings
│   ├── raft/
│   │   ├── RaftNode.java             ← Core Raft state machine
│   │   ├── RaftState.java            ← Enum: FOLLOWER, CANDIDATE, LEADER
│   │   ├── LogEntry.java             ← Raft log entry record
│   │   ├── RaftLog.java              ← Persistent log + compaction
│   │   ├── RaftRpcService.java       ← AppendEntries / RequestVote gRPC
│   │   ├── ElectionTimer.java        ← Randomized election timeout
│   │   └── ChunkPlacementFSM.java    ← FSM: applies committed entries
│   ├── ring/
│   │   └── ConsistentHashRing.java   ← Murmur3-based hash ring
│   ├── storage/
│   │   ├── ChunkStore.java           ← Local disk read/write
│   │   ├── ReplicationManager.java   ← RF=3 quorum write logic
│   │   └── ChecksumValidator.java    ← CRC32 data integrity checks
│   ├── server/
│   │   └── StorageGrpcService.java   ← PutObject / GetObject gRPC impl
│   ├── client/
│   │   └── StorageClient.java        ← SDK with hash ring + retry logic
│   └── proto/
│       └── storage.proto
├── src/test/java/com/storage/
│   ├── raft/
│   │   ├── RaftNodeTest.java
│   │   ├── RaftLogTest.java
│   │   └── ChunkPlacementFSMTest.java
│   ├── ring/
│   │   └── ConsistentHashRingTest.java
│   └── storage/
│       ├── ReplicationManagerTest.java
│       └── ChunkStoreTest.java
├── k8s/
│   ├── statefulset.yaml
│   ├── headless-service.yaml
│   └── configmap.yaml
└── scripts/
    └── cluster_bootstrap.sh
```

---

## 5. Build System — pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.storage</groupId>
    <artifactId>distributed-object-storage</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>17</java.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <grpc.version>1.59.0</grpc.version>
        <protobuf.version>3.25.0</protobuf.version>
        <spring-boot.version>3.2.0</spring-boot.version>
    </properties>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.0</version>
    </parent>

    <dependencies>
        <!-- Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>

        <!-- gRPC -->
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-netty-shaded</artifactId>
            <version>${grpc.version}</version>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-protobuf</artifactId>
            <version>${grpc.version}</version>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-stub</artifactId>
            <version>${grpc.version}</version>
        </dependency>

        <!-- Murmur3 hashing -->
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>32.1.3-jre</version>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
            <!-- Protobuf/gRPC codegen -->
            <plugin>
                <groupId>org.xolstice.maven.plugins</groupId>
                <artifactId>protobuf-maven-plugin</artifactId>
                <version>0.6.1</version>
                <configuration>
                    <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
                    <pluginId>grpc-java</pluginId>
                    <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>compile-custom</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 6. Core Implementation

### 6.1 Consistent Hashing Ring (Java 17)

```java
// ring/ConsistentHashRing.java
package com.storage.ring;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
                String virtualKey = nodeAddress + "#" + i;
                int hash = murmur3Hash(virtualKey);
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
                String virtualKey = nodeAddress + "#" + i;
                int hash = murmur3Hash(virtualKey);
                ring.remove(hash);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns the primary node for a given object key.
     * Uses clockwise traversal on the hash ring.
     */
    public String getNode(String objectKey) {
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) return null;

            int hash = murmur3Hash(objectKey);
            // Find first node clockwise from hash position
            Map.Entry<Integer, String> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();  // wrap around
            }
            return entry.getValue();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns RF distinct physical nodes for an object key.
     * Used for quorum-based replication (e.g., RF=3).
     */
    public List<String> getReplicaNodes(String objectKey, int replicationFactor) {
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) return Collections.emptyList();

            int hash = murmur3Hash(objectKey);
            Set<String> seen = new LinkedHashSet<>();
            NavigableMap<Integer, String> tailMap = ring.tailMap(hash, true);

            // Walk clockwise, collecting distinct physical nodes
            for (String node : tailMap.values()) {
                if (seen.size() >= replicationFactor) break;
                seen.add(node);
            }
            // Wrap around if needed
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
            Set<String> unique = new HashSet<>(ring.values());
            return unique.size();
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
```

### 6.2 Raft Node (Core State Machine — Java 17)

```java
// raft/RaftNode.java
package com.storage.raft;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;

    private final String nodeId;
    private final List<String> peers;
    private final ReentrantLock lock = new ReentrantLock();

    // ── Persistent state (must survive crashes — written to WAL before responding) ──
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private final RaftLog raftLog;

    // ── Volatile state ──
    private volatile RaftState state = RaftState.FOLLOWER;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // ── Leader-only volatile state ──
    private final ConcurrentHashMap<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> matchIndex = new ConcurrentHashMap<>();

    // ── Timers ──
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimerFuture;
    private ScheduledFuture<?> heartbeatFuture;

    // ── FSM ──
    private final ChunkPlacementFSM fsm;

    // ── RPC client for communicating with peers ──
    private final RaftRpcService rpcService;

    public RaftNode(String nodeId, List<String> peers,
                    RaftLog raftLog, ChunkPlacementFSM fsm,
                    RaftRpcService rpcService) {
        this.nodeId = nodeId;
        this.peers = List.copyOf(peers);
        this.raftLog = raftLog;
        this.fsm = fsm;
        this.rpcService = rpcService;
    }

    public void start() {
        resetElectionTimer();
        log.info("Raft node {} started as FOLLOWER", nodeId);
    }

    // ══════════════════════════════════════════════
    // ELECTION LOGIC
    // ══════════════════════════════════════════════

    private void resetElectionTimer() {
        if (electionTimerFuture != null) {
            electionTimerFuture.cancel(false);
        }
        int timeout = ELECTION_TIMEOUT_MIN_MS +
            ThreadLocalRandom.current().nextInt(
                ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        electionTimerFuture = scheduler.schedule(
            this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        lock.lock();
        try {
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            long term = currentTerm;
            long lastLogIndex = raftLog.lastIndex();
            long lastLogTerm = raftLog.lastTerm();
            log.info("Node {} starting election for term {}", nodeId, term);
            lock.unlock();

            AtomicInteger votes = new AtomicInteger(1); // vote for self
            int majority = (peers.size() + 1) / 2 + 1;

            CountDownLatch latch = new CountDownLatch(peers.size());

            for (String peer : peers) {
                CompletableFuture.runAsync(() -> {
                    try {
                        boolean granted = rpcService.sendRequestVote(
                            peer, term, nodeId, lastLogIndex, lastLogTerm);
                        if (granted && votes.incrementAndGet() >= majority) {
                            becomeLeader(term);
                        }
                    } catch (Exception e) {
                        log.warn("RequestVote to {} failed: {}", peer, e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Don't wait forever — election timeout will trigger new election
        } catch (Exception e) {
            lock.unlock();
            throw e;
        }
    }

    private void becomeLeader(long term) {
        lock.lock();
        try {
            if (currentTerm != term || state == RaftState.LEADER) return;
            state = RaftState.LEADER;
            log.info("Node {} became LEADER for term {}", nodeId, term);

            // Initialize nextIndex and matchIndex for all peers
            long lastIndex = raftLog.lastIndex();
            for (String peer : peers) {
                nextIndex.put(peer, lastIndex + 1);
                matchIndex.put(peer, 0L);
            }

            // Start periodic heartbeats
            if (heartbeatFuture != null) heartbeatFuture.cancel(false);
            heartbeatFuture = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);

            // Cancel election timer
            if (electionTimerFuture != null) electionTimerFuture.cancel(false);
        } finally {
            lock.unlock();
        }
    }

    // ══════════════════════════════════════════════
    // LOG REPLICATION
    // ══════════════════════════════════════════════

    private void sendHeartbeats() {
        if (state != RaftState.LEADER) return;

        for (String peer : peers) {
            CompletableFuture.runAsync(() -> {
                try {
                    long peerNextIdx = nextIndex.getOrDefault(peer, 1L);
                    long prevLogIndex = peerNextIdx - 1;
                    long prevLogTerm = prevLogIndex > 0 ?
                        raftLog.getEntry(prevLogIndex).term() : 0;

                    List<LogEntry> entries = raftLog.getEntriesFrom(peerNextIdx);

                    var reply = rpcService.sendAppendEntries(
                        peer, currentTerm, nodeId,
                        prevLogIndex, prevLogTerm,
                        entries, commitIndex);

                    lock.lock();
                    try {
                        if (reply.term() > currentTerm) {
                            stepDown(reply.term());
                            return;
                        }
                        if (reply.success()) {
                            long newMatchIdx = prevLogIndex + entries.size();
                            matchIndex.put(peer, newMatchIdx);
                            nextIndex.put(peer, newMatchIdx + 1);
                            advanceCommitIndex();
                        } else {
                            // Decrement nextIndex and retry
                            nextIndex.put(peer, Math.max(1, peerNextIdx - 1));
                        }
                    } finally {
                        lock.unlock();
                    }
                } catch (Exception e) {
                    log.warn("AppendEntries to {} failed: {}", peer, e.getMessage());
                }
            });
        }
    }

    /**
     * The leader can advance commitIndex to N if:
     * 1. N > commitIndex
     * 2. A majority of matchIndex[i] >= N
     * 3. log[N].term == currentTerm (safety requirement from Raft paper)
     */
    private void advanceCommitIndex() {
        long[] allMatchIndices = new long[peers.size() + 1];
        int i = 0;
        for (String peer : peers) {
            allMatchIndices[i++] = matchIndex.getOrDefault(peer, 0L);
        }
        allMatchIndices[i] = raftLog.lastIndex(); // leader's own index
        Arrays.sort(allMatchIndices);

        long median = allMatchIndices[allMatchIndices.length / 2];
        if (median > commitIndex && raftLog.getEntry(median).term() == currentTerm) {
            commitIndex = median;
            applyCommitted();
        }
    }

    // ══════════════════════════════════════════════
    // APPEND ENTRIES RPC HANDLER (called on followers)
    // ══════════════════════════════════════════════

    public record AppendEntriesReply(long term, boolean success) {}

    public AppendEntriesReply handleAppendEntries(
            long term, String leaderId,
            long prevLogIndex, long prevLogTerm,
            List<LogEntry> entries, long leaderCommit) {

        lock.lock();
        try {
            // Rule 1: Reply false if term < currentTerm
            if (term < currentTerm) {
                return new AppendEntriesReply(currentTerm, false);
            }

            // Valid leader — reset election timer
            resetElectionTimer();
            if (term > currentTerm) {
                stepDown(term);
            }

            // Rule 2: Reply false if log doesn't contain entry at prevLogIndex
            if (prevLogIndex > 0) {
                if (raftLog.lastIndex() < prevLogIndex) {
                    return new AppendEntriesReply(currentTerm, false);
                }
                if (raftLog.getEntry(prevLogIndex).term() != prevLogTerm) {
                    return new AppendEntriesReply(currentTerm, false);
                }
            }

            // Rule 3 & 4: Delete conflicting entries, append new ones
            raftLog.truncateFrom(prevLogIndex + 1);
            raftLog.appendAll(entries);

            // Rule 5: Update commitIndex
            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, raftLog.lastIndex());
                applyCommitted();
            }

            return new AppendEntriesReply(currentTerm, true);
        } finally {
            lock.unlock();
        }
    }

    // ══════════════════════════════════════════════
    // REQUEST VOTE RPC HANDLER
    // ══════════════════════════════════════════════

    public record RequestVoteReply(long term, boolean granted) {}

    public RequestVoteReply handleRequestVote(
            long candidateTerm, String candidateId,
            long lastLogIndex, long lastLogTerm) {

        lock.lock();
        try {
            if (candidateTerm < currentTerm) {
                return new RequestVoteReply(currentTerm, false);
            }

            if (candidateTerm > currentTerm) {
                stepDown(candidateTerm);
            }

            // Grant vote if:
            // 1. Haven't voted yet (or already voted for this candidate)
            // 2. Candidate's log is at least as up-to-date as ours
            boolean logUpToDate = (lastLogTerm > raftLog.lastTerm()) ||
                (lastLogTerm == raftLog.lastTerm() && lastLogIndex >= raftLog.lastIndex());

            boolean canVote = (votedFor == null || votedFor.equals(candidateId))
                              && logUpToDate;

            if (canVote) {
                votedFor = candidateId;
                resetElectionTimer();
                return new RequestVoteReply(currentTerm, true);
            }

            return new RequestVoteReply(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    // ══════════════════════════════════════════════
    // APPLY COMMITTED ENTRIES TO FSM
    // ══════════════════════════════════════════════

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            fsm.apply(entry.command());
            log.debug("Applied log entry {} to FSM", lastApplied);
        }
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        if (heartbeatFuture != null) heartbeatFuture.cancel(false);
        resetElectionTimer();
    }

    // ══════════════════════════════════════════════
    // CLIENT API — Propose a new chunk placement
    // ══════════════════════════════════════════════

    /**
     * Only the leader can accept proposals.
     * Appends to local log, replication happens via heartbeats.
     */
    public CompletableFuture<Boolean> propose(byte[] command) {
        if (state != RaftState.LEADER) {
            return CompletableFuture.completedFuture(false);
        }

        lock.lock();
        try {
            LogEntry entry = new LogEntry(currentTerm, raftLog.lastIndex() + 1, command);
            raftLog.append(entry);
        } finally {
            lock.unlock();
        }

        // Replication to followers happens via sendHeartbeats()
        // Return a future that completes when entry is committed
        return CompletableFuture.supplyAsync(() -> {
            long targetIndex = raftLog.lastIndex();
            // Wait for commitIndex to reach our entry
            while (commitIndex < targetIndex) {
                try { Thread.sleep(10); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        });
    }

    // ── Getters ──
    public RaftState getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public String getNodeId() { return nodeId; }
    public boolean isLeader() { return state == RaftState.LEADER; }
}
```

### 6.3 Log Entry & Raft Log

```java
// raft/LogEntry.java
package com.storage.raft;

/**
 * Immutable Raft log entry. Uses Java 17 record.
 */
public record LogEntry(long term, long index, byte[] command) {}

// raft/RaftState.java
package com.storage.raft;

public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
```

```java
// raft/RaftLog.java
package com.storage.raft;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Persistent Raft log. In production, this would write to disk (WAL).
 * For simplicity, this implementation is in-memory with snapshot support.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private long snapshotLastIndex = 0;
    private long snapshotLastTerm = 0;

    public long lastIndex() {
        rwLock.readLock().lock();
        try {
            return entries.isEmpty() ? snapshotLastIndex :
                entries.get(entries.size() - 1).index();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long lastTerm() {
        rwLock.readLock().lock();
        try {
            return entries.isEmpty() ? snapshotLastTerm :
                entries.get(entries.size() - 1).term();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public LogEntry getEntry(long index) {
        rwLock.readLock().lock();
        try {
            int offset = (int)(index - snapshotLastIndex - 1);
            if (offset < 0 || offset >= entries.size()) {
                throw new IndexOutOfBoundsException("Log index " + index + " not available");
            }
            return entries.get(offset);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void append(LogEntry entry) {
        rwLock.writeLock().lock();
        try {
            entries.add(entry);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void appendAll(List<LogEntry> newEntries) {
        rwLock.writeLock().lock();
        try {
            entries.addAll(newEntries);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<LogEntry> getEntriesFrom(long startIndex) {
        rwLock.readLock().lock();
        try {
            int offset = (int)(startIndex - snapshotLastIndex - 1);
            if (offset < 0) offset = 0;
            if (offset >= entries.size()) return Collections.emptyList();
            return new ArrayList<>(entries.subList(offset, entries.size()));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void truncateFrom(long index) {
        rwLock.writeLock().lock();
        try {
            int offset = (int)(index - snapshotLastIndex - 1);
            if (offset >= 0 && offset < entries.size()) {
                entries.subList(offset, entries.size()).clear();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Log compaction: discard entries up to snapshotIndex.
     * Called after FSM snapshot is persisted to disk.
     */
    public void compact(long snapshotIndex, long snapshotTerm) {
        rwLock.writeLock().lock();
        try {
            int offset = (int)(snapshotIndex - snapshotLastIndex);
            if (offset > 0 && offset <= entries.size()) {
                entries.subList(0, offset).clear();
            }
            snapshotLastIndex = snapshotIndex;
            snapshotLastTerm = snapshotTerm;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
```

### 6.4 Chunk Placement FSM

```java
// raft/ChunkPlacementFSM.java
package com.storage.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkPlacementFSM {

    private static final Logger log = LoggerFactory.getLogger(ChunkPlacementFSM.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // objectKey → list of node addresses holding replicas
    private final ConcurrentHashMap<String, List<String>> chunkMap = new ConcurrentHashMap<>();

    public record ChunkPlacementCommand(
        String type,         // "place", "delete", "rebalance"
        String objectKey,
        String chunkId,
        List<String> nodes   // replica node addresses
    ) {}

    public void apply(byte[] commandData) {
        try {
            var cmd = mapper.readValue(commandData, ChunkPlacementCommand.class);
            switch (cmd.type()) {
                case "place" -> {
                    chunkMap.put(cmd.objectKey(), List.copyOf(cmd.nodes()));
                    log.debug("Placed chunk {} on nodes {}", cmd.objectKey(), cmd.nodes());
                }
                case "delete" -> {
                    chunkMap.remove(cmd.objectKey());
                    log.debug("Deleted chunk {}", cmd.objectKey());
                }
                case "rebalance" -> {
                    chunkMap.put(cmd.objectKey(), List.copyOf(cmd.nodes()));
                    log.debug("Rebalanced chunk {} to nodes {}", cmd.objectKey(), cmd.nodes());
                }
                default -> log.warn("Unknown command type: {}", cmd.type());
            }
        } catch (Exception e) {
            log.error("Failed to apply FSM command", e);
        }
    }

    public byte[] snapshot() {
        try {
            return mapper.writeValueAsBytes(chunkMap);
        } catch (Exception e) {
            throw new RuntimeException("Snapshot serialization failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    public void restore(byte[] data) {
        try {
            Map<String, List<String>> restored = mapper.readValue(data, Map.class);
            chunkMap.clear();
            chunkMap.putAll(restored);
            log.info("Restored FSM with {} chunks", chunkMap.size());
        } catch (Exception e) {
            throw new RuntimeException("Snapshot restoration failed", e);
        }
    }

    public List<String> getNodes(String objectKey) {
        return chunkMap.getOrDefault(objectKey, Collections.emptyList());
    }

    public int chunkCount() {
        return chunkMap.size();
    }
}
```

### 6.5 Replication Manager (RF=3 Quorum Write)

```java
// storage/ReplicationManager.java
package com.storage.storage;

import com.storage.ring.ConsistentHashRing;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int REPLICATION_FACTOR = 3;
    private static final int QUORUM = 2;  // majority of 3
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

    /**
     * Writes data to RF=3 nodes, waits for quorum (2/3) acknowledgment.
     * Returns true if quorum was met.
     */
    public boolean putObject(String key, byte[] data) throws Exception {
        List<String> replicaNodes = ring.getReplicaNodes(key, REPLICATION_FACTOR);
        if (replicaNodes.size() < QUORUM) {
            throw new IllegalStateException(
                "Insufficient nodes for quorum write: need " + QUORUM +
                ", available " + replicaNodes.size());
        }

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch quorumLatch = new CountDownLatch(QUORUM);
        List<Future<?>> futures = new ArrayList<>();

        for (String nodeAddr : replicaNodes) {
            futures.add(replicationExecutor.submit(() -> {
                try {
                    boolean written;
                    if (nodeAddr.equals(localAddress)) {
                        written = localStore.write(key, data);
                    } else {
                        written = getClient(nodeAddr).writeChunk(key, data);
                    }

                    if (written) {
                        successCount.incrementAndGet();
                        quorumLatch.countDown();
                    }
                } catch (Exception e) {
                    log.warn("Replication to {} failed for key {}: {}",
                             nodeAddr, key, e.getMessage());
                }
            }));
        }

        // Wait for quorum with timeout
        boolean quorumMet = quorumLatch.await(REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (!quorumMet || successCount.get() < QUORUM) {
            log.error("Quorum write failed for key {}: only {}/{} nodes acknowledged",
                       key, successCount.get(), REPLICATION_FACTOR);
            // Initiate async cleanup of partial writes
            cleanupPartialWrites(key, replicaNodes);
            return false;
        }

        log.info("Quorum write succeeded for key {}: {}/{} nodes",
                  key, successCount.get(), REPLICATION_FACTOR);
        return true;
    }

    /**
     * Read from the primary node. If unavailable, try replicas.
     */
    public Optional<byte[]> getObject(String key) {
        List<String> nodes = ring.getReplicaNodes(key, REPLICATION_FACTOR);

        for (String nodeAddr : nodes) {
            try {
                if (nodeAddr.equals(localAddress)) {
                    byte[] data = localStore.read(key);
                    if (data != null) return Optional.of(data);
                } else {
                    byte[] data = getClient(nodeAddr).readChunk(key);
                    if (data != null) return Optional.of(data);
                }
            } catch (Exception e) {
                log.warn("Read from {} failed for key {}, trying next replica",
                          nodeAddr, key);
            }
        }
        return Optional.empty();
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
                } catch (Exception ignored) {}
            }
        });
    }

    private StorageGrpcClient getClient(String nodeAddr) {
        return clients.computeIfAbsent(nodeAddr, addr -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
                .usePlaintext()
                .build();
            return new StorageGrpcClient(channel);
        });
    }
}
```

---

## 7. Kubernetes Deployment

### 7.1 StatefulSet

```yaml
# k8s/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: storage-node
  labels:
    app: distributed-storage
spec:
  serviceName: "storage-headless"
  replicas: 3
  selector:
    matchLabels:
      app: storage-node
  template:
    metadata:
      labels:
        app: storage-node
    spec:
      containers:
      - name: storage-node
        image: yourusername/storage-node:latest
        ports:
        - containerPort: 8080
          name: grpc
        - containerPort: 8081
          name: raft
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: PEERS
          value: "storage-node-0.storage-headless:8081,storage-node-1.storage-headless:8081,storage-node-2.storage-headless:8081"
        - name: DATA_DIR
          value: "/data"
        - name: GRPC_PORT
          value: "8080"
        - name: RAFT_PORT
          value: "8081"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        volumeMounts:
        - name: data
          mountPath: /data
        livenessProbe:
          grpc:
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          grpc:
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

### 7.2 Headless Service

```yaml
# k8s/headless-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: storage-headless
spec:
  clusterIP: None    # headless — DNS resolves to individual pod IPs
  selector:
    app: storage-node
  ports:
  - port: 8080
    name: grpc
  - port: 8081
    name: raft
```

### 7.3 Dockerfile

```dockerfile
# Dockerfile
FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven && \
    mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
RUN mkdir -p /data
EXPOSE 8080 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

---

## 8. Failure Modes & Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| **Leader crash** | Followers' election timeout fires (150-300ms) | New election; majority partition elects new leader; old leader steps down when it sees higher term |
| **Follower crash** | Leader's AppendEntries times out | Leader continues with remaining followers; quorum writes still succeed with 2/3 nodes |
| **Network partition (leader isolated)** | Leader can't get quorum ACK | Majority side elects new leader; isolated leader stops committing; partition heals → old leader sees higher term, steps down |
| **Split brain** | Impossible with strict majority quorum | Raft guarantees at most one leader per term — a leader needs majority votes |
| **Data corruption** | CRC32 checksum validation on reads | Re-replicate from healthy replica; Raft log replay for chunk map |
| **Disk full** | Chunk store write fails | Node marks itself unhealthy via K8s readiness probe; traffic routed to replicas |

---

## 9. Key Technical Interview Questions & Answers

### Architecture & Design

**Q1: Why use Raft for chunk placement but consistent hashing for routing? Aren't they redundant?**
> No — they serve different layers. Raft is the **control plane**: it ensures all nodes agree on the authoritative chunk map (which node owns which chunk). This is **replicated state** that must be consistent. Consistent hashing is the **data plane routing shortcut**: instead of asking the Raft leader for every read, the client locally computes which node to hit. If the node doesn't have the chunk (e.g., after rebalancing), it redirects to the correct node.

**Q2: Why Java 17 for a distributed storage system? What Java 17 features do you leverage?**
> Java 17 provides: (1) **Records** for immutable value types (`LogEntry`, `AppendEntriesReply`) — reduced boilerplate. (2) **Sealed classes** for Raft state transitions. (3) **Pattern matching** for cleaner FSM command dispatch. (4) **Better GC** (ZGC/Shenandoah) for low-latency storage operations. (5) **Foreign Function API (incubator)** for potential direct I/O integration.

**Q3: How does your system handle a "hot key" where one object gets disproportionate traffic?**
> Virtual nodes in the consistent hash ring already distribute load, but a single hot key still hits the same physical nodes. Mitigation strategies: (1) **Read replicas** — serve reads from any of the 3 replica nodes, not just the primary. (2) **Client-side caching** — cache hot objects in the SDK. (3) **Request rate limiting** per key at the storage node level.

### Raft Deep Dive

**Q4: What is the election timeout range and why is it randomized?**
> 150–300ms, as specified in the Raft paper. Randomization prevents **split votes** where multiple candidates start elections simultaneously. Without randomization, symmetrical timeouts would cause repeated failed elections (livelock). The randomized window ensures one candidate almost always starts earlier and wins.

**Q5: Explain the safety constraint in `advanceCommitIndex`. Why check `log[N].term == currentTerm`?**
> This prevents a subtle bug from the Raft paper (Figure 8). A new leader cannot commit entries from previous terms by counting replicas alone — it must first commit an entry from its **own** term. Without this check, a committed entry could be overwritten if a leader crashes and a follower with an older log becomes leader. By requiring `term == currentTerm`, the leader only commits its own entries, and previous-term entries are implicitly committed as a side effect.

**Q6: How do you handle log compaction?**
> The FSM implements `snapshot()` which serializes the entire chunk map to JSON. After the snapshot is persisted to disk, `RaftLog.compact()` discards all log entries before the snapshot index. On restart, the node restores the FSM from the latest snapshot and replays only the delta log entries after it. This prevents unbounded log growth.

**Q7: What happens during a network partition where the leader is isolated?**
> The isolated leader can no longer get quorum acknowledgment for new log entries, so it **stops committing**. The majority partition holds a new election after the election timeout. When the partition heals, the old leader sees a higher term in AppendEntries replies, **steps down to Follower**, and syncs its log from the new leader. Any uncommitted entries on the old leader are safely discarded.

### Consistent Hashing

**Q8: Why 150 virtual nodes per physical node?**
> With K virtual nodes per N physical nodes, the standard deviation of load imbalance is approximately `O(1/sqrt(K))`. At 150 virtual nodes, each physical node gets roughly `150/TotalVirtualNodes` of the key space, which keeps the maximum deviation under 5%. Fewer virtual nodes (e.g., 10) would cause 30%+ imbalance; more (e.g., 500) wastes memory with diminishing returns.

**Q9: What happens when a node joins or leaves the cluster?**
> **Join**: `addNode()` inserts 150 virtual node hashes into the `TreeMap`. Keys that now hash between the new node's virtual nodes and the next clockwise node migrate to the new node. Only ~`1/N` of keys need to move (where N = total nodes). **Leave**: `removeNode()` deletes virtual nodes; affected keys are now served by the next clockwise node. The Raft leader proposes the membership change, and all nodes update their rings.

### Replication & Durability

**Q10: Explain the quorum write strategy. Why 2/3 and not 3/3?**
> Quorum = majority = `ceil(RF/2)`. With RF=3, quorum is 2. This ensures **any two quorums overlap by at least one node** — so a subsequent quorum read is guaranteed to see the latest write. Writing to all 3 nodes would reduce availability (one node failure blocks all writes). The tradeoff: 2/3 gives fault tolerance (1 node can fail) at the cost of eventually needing anti-entropy to repair the lagging replica.

**Q11: How do you ensure data integrity across replicas?**
> `ChecksumValidator` computes CRC32C checksums for every chunk on write. The checksum is stored alongside the data. On read, the checksum is revalidated. Background anti-entropy processes periodically compare checksums across replicas using Merkle trees and repair inconsistencies by re-replicating from a healthy replica.

**Q12: What happens if a quorum write partially succeeds (1/3) and then the client retries?**
> The retry will attempt to write to the same 3 nodes (consistent hashing is deterministic). The node that already has the data returns success immediately (idempotent write — check if chunk already exists with matching checksum). The cleanup mechanism also runs asynchronously for failed writes to avoid orphaned partial data.

### Operations

**Q13: Why StatefulSets instead of Deployments for Kubernetes?**
> StatefulSets provide: (1) **Stable network identity** — `storage-node-0`, `storage-node-1` etc. — which Raft requires for peer addressing. (2) **Persistent volume claims** — each pod gets its own PVC that survives pod restarts. (3) **Ordered deployment** — nodes come up sequentially, which simplifies Raft cluster bootstrap. Deployments would give random pod names and ephemeral storage, breaking both Raft and data persistence.

**Q14: How does a new node bootstrap into an existing cluster?**
> Three phases: (1) **Join Raft cluster** — the leader proposes a configuration change adding the new node. (2) **Snapshot transfer** — the leader sends the latest FSM snapshot to the new node (InstallSnapshot RPC). (3) **Ring update** — after FSM is restored, the new node is added to all clients' consistent hash rings. Keys are lazily migrated as new writes route to the new node.

**Q15: How would you monitor this cluster in production?**
> Key metrics: (1) **Raft**: current term, leader identity, log size, commit latency. (2) **Storage**: disk utilization, read/write IOPS, checksum failures. (3) **Replication**: quorum success rate, replication lag. (4) **Ring**: node distribution entropy, hot key detection. All exposed as Prometheus metrics via Spring Boot Actuator with Micrometer.

---
