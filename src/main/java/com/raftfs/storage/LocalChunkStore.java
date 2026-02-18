package com.raftfs.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

public class LocalChunkStore implements ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(LocalChunkStore.class);

    private final Path dataDir;
    private final ChecksumValidator checksumValidator;
    private final ConcurrentHashMap<String, Long> checksumMap = new ConcurrentHashMap<>();

    public LocalChunkStore(String dataDir) {
        this.dataDir = Path.of(dataDir);
        this.checksumValidator = new ChecksumValidator();
        try {
            Files.createDirectories(this.dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data directory: " + dataDir, e);
        }
    }

    @Override
    public boolean write(String key, byte[] data) {
        try {
            Path filePath = resolveKeyPath(key);
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            checksumMap.put(key, checksumValidator.compute(data));
            return true;
        } catch (IOException e) {
            log.error("Failed to write key {}: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public byte[] read(String key) {
        try {
            Path filePath = resolveKeyPath(key);
            if (!Files.exists(filePath)) return null;

            byte[] data = Files.readAllBytes(filePath);
            Long expectedChecksum = checksumMap.get(key);
            if (expectedChecksum != null && !checksumValidator.validate(data, expectedChecksum)) {
                log.error("Checksum mismatch for key {}", key);
                return null;
            }
            return data;
        } catch (IOException e) {
            log.error("Failed to read key {}: {}", key, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            Path filePath = resolveKeyPath(key);
            checksumMap.remove(key);
            return Files.deleteIfExists(filePath);
        } catch (IOException e) {
            log.error("Failed to delete key {}: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean exists(String key) {
        return Files.exists(resolveKeyPath(key));
    }

    private Path resolveKeyPath(String key) {
        String safeName = key.replace("/", "_").replace("\\", "_");
        return dataDir.resolve(safeName);
    }
}
