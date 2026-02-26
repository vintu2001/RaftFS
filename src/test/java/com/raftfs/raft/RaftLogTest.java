package com.raftfs.raft;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    private RaftLog raftLog;

    @BeforeEach
    void setUp() {
        raftLog = new RaftLog();
    }

    @Test
    void emptyLogReturnsZeroIndex() {
        assertEquals(0, raftLog.lastIndex());
        assertEquals(0, raftLog.lastTerm());
    }

    @Test
    void appendAndRetrieveEntry() {
        LogEntry entry = new LogEntry(1, 1, "test".getBytes());
        raftLog.append(entry);

        assertEquals(1, raftLog.lastIndex());
        assertEquals(1, raftLog.lastTerm());

        LogEntry retrieved = raftLog.getEntry(1);
        assertEquals(1, retrieved.term());
        assertEquals(1, retrieved.index());
    }

    @Test
    void appendMultipleEntries() {
        raftLog.appendAll(List.of(
                new LogEntry(1, 1, "cmd1".getBytes()),
                new LogEntry(1, 2, "cmd2".getBytes()),
                new LogEntry(2, 3, "cmd3".getBytes())
        ));

        assertEquals(3, raftLog.lastIndex());
        assertEquals(2, raftLog.lastTerm());
    }

    @Test
    void getEntriesFromIndex() {
        raftLog.appendAll(List.of(
                new LogEntry(1, 1, "a".getBytes()),
                new LogEntry(1, 2, "b".getBytes()),
                new LogEntry(1, 3, "c".getBytes())
        ));

        List<LogEntry> entries = raftLog.getEntriesFrom(2);
        assertEquals(2, entries.size());
        assertEquals(2, entries.get(0).index());
    }

    @Test
    void truncateFromIndex() {
        raftLog.appendAll(List.of(
                new LogEntry(1, 1, "a".getBytes()),
                new LogEntry(1, 2, "b".getBytes()),
                new LogEntry(1, 3, "c".getBytes())
        ));

        raftLog.truncateFrom(2);
        assertEquals(1, raftLog.lastIndex());
    }

    @Test
    void compactDiscardsPrefixEntries() {
        raftLog.appendAll(List.of(
                new LogEntry(1, 1, "a".getBytes()),
                new LogEntry(1, 2, "b".getBytes()),
                new LogEntry(2, 3, "c".getBytes())
        ));

        raftLog.compact(2, 1);
        assertEquals(3, raftLog.lastIndex());

        LogEntry entry = raftLog.getEntry(3);
        assertEquals(2, entry.term());
    }

    @Test
    void getEntryThrowsForInvalidIndex() {
        assertThrows(IndexOutOfBoundsException.class, () -> raftLog.getEntry(1));
    }

    @Test
    void getEntriesFromBeyondLastReturnsEmpty() {
        raftLog.append(new LogEntry(1, 1, "a".getBytes()));
        List<LogEntry> entries = raftLog.getEntriesFrom(5);
        assertTrue(entries.isEmpty());
    }
}
