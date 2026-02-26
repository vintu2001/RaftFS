package com.raftfs.raft;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class RaftNodeTest {

    @Mock
    private RaftRpcService rpcService;

    private RaftLog raftLog;
    private ChunkPlacementFSM fsm;
    private RaftNode node;

    @BeforeEach
    void setUp() {
        raftLog = new RaftLog();
        fsm = new ChunkPlacementFSM();
        node = new RaftNode("node-0", List.of("node-1", "node-2"), raftLog, fsm, rpcService);
    }

    @Test
    void nodeStartsAsFollower() {
        assertEquals(RaftState.FOLLOWER, node.getState());
        assertEquals(0, node.getCurrentTerm());
    }

    @Test
    void handleRequestVoteGrantsVoteForHigherTerm() {
        RaftNode.RequestVoteReply reply = node.handleRequestVote(1, "node-1", 0, 0);
        assertTrue(reply.granted());
        assertEquals(1, node.getCurrentTerm());
    }

    @Test
    void handleRequestVoteRejectsLowerTerm() {
        node.handleRequestVote(5, "node-1", 0, 0);
        RaftNode.RequestVoteReply reply = node.handleRequestVote(3, "node-2", 0, 0);
        assertFalse(reply.granted());
    }

    @Test
    void handleRequestVoteRejectsDuplicateVoteForDifferentCandidate() {
        node.handleRequestVote(1, "node-1", 0, 0);
        RaftNode.RequestVoteReply reply = node.handleRequestVote(1, "node-2", 0, 0);
        assertFalse(reply.granted());
    }

    @Test
    void handleAppendEntriesRejectsLowerTerm() {
        node.handleRequestVote(5, "node-1", 0, 0);
        RaftNode.AppendEntriesReply reply = node.handleAppendEntries(
                3, "node-1", 0, 0, Collections.emptyList(), 0);
        assertFalse(reply.success());
    }

    @Test
    void handleAppendEntriesAcceptsValidRequest() {
        RaftNode.AppendEntriesReply reply = node.handleAppendEntries(
                1, "node-1", 0, 0, Collections.emptyList(), 0);
        assertTrue(reply.success());
        assertEquals(1, node.getCurrentTerm());
    }

    @Test
    void handleAppendEntriesAppendsEntries() {
        List<LogEntry> entries = List.of(
                new LogEntry(1, 1, "cmd1".getBytes()),
                new LogEntry(1, 2, "cmd2".getBytes())
        );

        RaftNode.AppendEntriesReply reply = node.handleAppendEntries(
                1, "node-1", 0, 0, entries, 0);
        assertTrue(reply.success());
        assertEquals(2, raftLog.lastIndex());
    }

    @Test
    void handleAppendEntriesUpdatesCommitIndex() {
        List<LogEntry> entries = List.of(new LogEntry(1, 1, "{}".getBytes()));
        node.handleAppendEntries(1, "node-1", 0, 0, entries, 1);
        assertEquals(1, node.getCommitIndex());
    }

    @Test
    void handleAppendEntriesRejectsWithMismatchedPrevLog() {
        List<LogEntry> entries = List.of(new LogEntry(1, 1, "cmd".getBytes()));
        node.handleAppendEntries(1, "node-1", 0, 0, entries, 0);

        RaftNode.AppendEntriesReply reply = node.handleAppendEntries(
                1, "node-1", 1, 2, Collections.emptyList(), 0);
        assertFalse(reply.success());
    }

    @Test
    void proposeFailsWhenNotLeader() throws Exception {
        boolean result = node.propose("test".getBytes()).get();
        assertFalse(result);
    }

    @Test
    void shutdownCompletesCleanly() {
        node.start();
        node.shutdown();
        assertNotNull(node.getNodeId());
    }
}
