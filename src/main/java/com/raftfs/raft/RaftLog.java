package com.raftfs.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private long snapshotLastIndex = 0;
    private long snapshotLastTerm = 0;

    public long lastIndex() {
        rwLock.readLock().lock();
        try {
            return entries.isEmpty() ? snapshotLastIndex
                    : entries.get(entries.size() - 1).index();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long lastTerm() {
        rwLock.readLock().lock();
        try {
            return entries.isEmpty() ? snapshotLastTerm
                    : entries.get(entries.size() - 1).term();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public LogEntry getEntry(long index) {
        rwLock.readLock().lock();
        try {
            int offset = (int) (index - snapshotLastIndex - 1);
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
            int offset = (int) (startIndex - snapshotLastIndex - 1);
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
            int offset = (int) (index - snapshotLastIndex - 1);
            if (offset >= 0 && offset < entries.size()) {
                entries.subList(offset, entries.size()).clear();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void compact(long snapshotIndex, long snapshotTerm) {
        rwLock.writeLock().lock();
        try {
            int offset = (int) (snapshotIndex - snapshotLastIndex);
            if (offset > 0 && offset <= entries.size()) {
                entries.subList(0, offset).clear();
            }
            this.snapshotLastIndex = snapshotIndex;
            this.snapshotLastTerm = snapshotTerm;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long getSnapshotLastIndex() {
        return snapshotLastIndex;
    }

    public long getSnapshotLastTerm() {
        return snapshotLastTerm;
    }
}
