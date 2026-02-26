package com.raftfs.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ChunkStoreTest {

    @TempDir
    Path tempDir;

    private LocalChunkStore store;

    @BeforeEach
    void setUp() {
        store = new LocalChunkStore(tempDir.toString());
    }

    @Test
    void writeAndReadData() {
        byte[] data = "test data".getBytes();
        assertTrue(store.write("key1", data));

        byte[] result = store.read("key1");
        assertArrayEquals(data, result);
    }

    @Test
    void readReturnsNullForMissingKey() {
        assertNull(store.read("nonexistent"));
    }

    @Test
    void deleteRemovesData() {
        store.write("key1", "data".getBytes());
        assertTrue(store.delete("key1"));
        assertNull(store.read("key1"));
    }

    @Test
    void existsReturnsTrueForExistingKey() {
        store.write("key1", "data".getBytes());
        assertTrue(store.exists("key1"));
    }

    @Test
    void existsReturnsFalseForMissingKey() {
        assertFalse(store.exists("nonexistent"));
    }

    @Test
    void overwriteExistingKey() {
        store.write("key1", "old".getBytes());
        store.write("key1", "new".getBytes());

        assertArrayEquals("new".getBytes(), store.read("key1"));
    }

    @Test
    void writeLargeData() {
        byte[] data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        assertTrue(store.write("large-key", data));
        assertArrayEquals(data, store.read("large-key"));
    }

    @Test
    void deleteNonexistentKeyReturnsFalse() {
        assertFalse(store.delete("nonexistent"));
    }
}
