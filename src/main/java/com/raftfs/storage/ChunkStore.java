package com.raftfs.storage;

public interface ChunkStore {
    boolean write(String key, byte[] data);
    byte[] read(String key);
    boolean delete(String key);
    boolean exists(String key);
}
