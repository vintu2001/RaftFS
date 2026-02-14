package com.raftfs.raft;

public record LogEntry(long term, long index, byte[] command) {}
