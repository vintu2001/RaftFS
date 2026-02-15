package com.raftfs.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final List<String> peers;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private final RaftLog raftLog;

    private volatile RaftState state = RaftState.FOLLOWER;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private final ConcurrentHashMap<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> matchIndex = new ConcurrentHashMap<>();

    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "heartbeat");
                t.setDaemon(true);
                return t;
            });
    private ScheduledFuture<?> heartbeatFuture;

    private final ElectionTimer electionTimer;
    private final ChunkPlacementFSM fsm;
    private final RaftRpcService rpcService;
    private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(8);

    public RaftNode(String nodeId, List<String> peers, RaftLog raftLog,
                    ChunkPlacementFSM fsm, RaftRpcService rpcService) {
        this.nodeId = nodeId;
        this.peers = List.copyOf(peers);
        this.raftLog = raftLog;
        this.fsm = fsm;
        this.rpcService = rpcService;
        this.electionTimer = new ElectionTimer(this::startElection);
    }

    public void start() {
        electionTimer.reset();
        log.info("Raft node {} started as FOLLOWER", nodeId);
    }

    public void shutdown() {
        electionTimer.shutdown();
        if (heartbeatFuture != null) heartbeatFuture.cancel(false);
        heartbeatScheduler.shutdownNow();
        rpcExecutor.shutdownNow();
    }

    private void startElection() {
        lock.lock();
        long term;
        long lastLogIndex;
        long lastLogTerm;
        try {
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            term = currentTerm;
            lastLogIndex = raftLog.lastIndex();
            lastLogTerm = raftLog.lastTerm();
            log.info("Node {} starting election for term {}", nodeId, term);
        } finally {
            lock.unlock();
        }

        AtomicInteger votes = new AtomicInteger(1);
        int majority = (peers.size() + 1) / 2 + 1;

        for (String peer : peers) {
            rpcExecutor.submit(() -> {
                boolean granted = rpcService.sendRequestVote(
                        peer, term, nodeId, lastLogIndex, lastLogTerm);
                if (granted && votes.incrementAndGet() >= majority) {
                    becomeLeader(term);
                }
            });
        }

        electionTimer.reset();
    }

    private void becomeLeader(long term) {
        lock.lock();
        try {
            if (currentTerm != term || state == RaftState.LEADER) return;
            state = RaftState.LEADER;
            log.info("Node {} became LEADER for term {}", nodeId, term);

            long lastIndex = raftLog.lastIndex();
            for (String peer : peers) {
                nextIndex.put(peer, lastIndex + 1);
                matchIndex.put(peer, 0L);
            }

            electionTimer.cancel();
            if (heartbeatFuture != null) heartbeatFuture.cancel(false);
            heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(
                    this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void sendHeartbeats() {
        if (state != RaftState.LEADER) return;

        for (String peer : peers) {
            rpcExecutor.submit(() -> replicateLog(peer));
        }
    }

    private void replicateLog(String peer) {
        try {
            long peerNextIdx = nextIndex.getOrDefault(peer, 1L);
            long prevLogIndex = peerNextIdx - 1;
            long prevLogTerm = prevLogIndex > 0 ? raftLog.getEntry(prevLogIndex).term() : 0;

            List<LogEntry> entries = raftLog.getEntriesFrom(peerNextIdx);

            RaftRpcService.AppendEntriesResult result = rpcService.sendAppendEntries(
                    peer, currentTerm, nodeId, prevLogIndex, prevLogTerm,
                    entries, commitIndex);

            lock.lock();
            try {
                if (result.term() > currentTerm) {
                    stepDown(result.term());
                    return;
                }
                if (result.success()) {
                    long newMatchIdx = prevLogIndex + entries.size();
                    matchIndex.put(peer, newMatchIdx);
                    nextIndex.put(peer, newMatchIdx + 1);
                    advanceCommitIndex();
                } else {
                    nextIndex.put(peer, Math.max(1, peerNextIdx - 1));
                }
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            log.warn("Replication to {} failed: {}", peer, e.getMessage());
        }
    }

    private void advanceCommitIndex() {
        long[] indices = new long[peers.size() + 1];
        int i = 0;
        for (String peer : peers) {
            indices[i++] = matchIndex.getOrDefault(peer, 0L);
        }
        indices[i] = raftLog.lastIndex();
        Arrays.sort(indices);

        long median = indices[indices.length / 2];
        if (median > commitIndex && raftLog.getEntry(median).term() == currentTerm) {
            commitIndex = median;
            applyCommitted();
        }
    }

    public record AppendEntriesReply(long term, boolean success) {}

    public AppendEntriesReply handleAppendEntries(long term, String leaderId,
                                                   long prevLogIndex, long prevLogTerm,
                                                   List<LogEntry> entries, long leaderCommit) {
        lock.lock();
        try {
            if (term < currentTerm) {
                return new AppendEntriesReply(currentTerm, false);
            }

            electionTimer.reset();

            if (term > currentTerm) {
                stepDown(term);
            }

            if (prevLogIndex > 0) {
                if (raftLog.lastIndex() < prevLogIndex) {
                    return new AppendEntriesReply(currentTerm, false);
                }
                if (raftLog.getEntry(prevLogIndex).term() != prevLogTerm) {
                    return new AppendEntriesReply(currentTerm, false);
                }
            }

            raftLog.truncateFrom(prevLogIndex + 1);
            raftLog.appendAll(entries);

            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, raftLog.lastIndex());
                applyCommitted();
            }

            return new AppendEntriesReply(currentTerm, true);
        } finally {
            lock.unlock();
        }
    }

    public record RequestVoteReply(long term, boolean granted) {}

    public RequestVoteReply handleRequestVote(long candidateTerm, String candidateId,
                                              long lastLogIndex, long lastLogTerm) {
        lock.lock();
        try {
            if (candidateTerm < currentTerm) {
                return new RequestVoteReply(currentTerm, false);
            }

            if (candidateTerm > currentTerm) {
                stepDown(candidateTerm);
            }

            boolean logUpToDate = (lastLogTerm > raftLog.lastTerm()) ||
                    (lastLogTerm == raftLog.lastTerm() && lastLogIndex >= raftLog.lastIndex());

            boolean canVote = (votedFor == null || votedFor.equals(candidateId)) && logUpToDate;

            if (canVote) {
                votedFor = candidateId;
                electionTimer.reset();
                return new RequestVoteReply(currentTerm, true);
            }

            return new RequestVoteReply(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            fsm.apply(entry.command());
        }
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        if (heartbeatFuture != null) heartbeatFuture.cancel(false);
        electionTimer.reset();
    }

    public CompletableFuture<Boolean> propose(byte[] command) {
        if (state != RaftState.LEADER) {
            return CompletableFuture.completedFuture(false);
        }

        long targetIndex;
        lock.lock();
        try {
            LogEntry entry = new LogEntry(currentTerm, raftLog.lastIndex() + 1, command);
            raftLog.append(entry);
            targetIndex = entry.index();
        } finally {
            lock.unlock();
        }

        return CompletableFuture.supplyAsync(() -> {
            long deadline = System.currentTimeMillis() + 5000;
            while (commitIndex < targetIndex) {
                if (System.currentTimeMillis() > deadline) return false;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        });
    }

    public RaftState getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public String getNodeId() { return nodeId; }
    public boolean isLeader() { return state == RaftState.LEADER; }
    public long getCommitIndex() { return commitIndex; }
}
