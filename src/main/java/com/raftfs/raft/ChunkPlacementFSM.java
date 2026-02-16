package com.raftfs.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkPlacementFSM {

    private static final Logger log = LoggerFactory.getLogger(ChunkPlacementFSM.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, List<String>> chunkMap = new ConcurrentHashMap<>();

    public record ChunkPlacementCommand(
            String type,
            String objectKey,
            String chunkId,
            List<String> nodes
    ) {}

    public void apply(byte[] commandData) {
        try {
            ChunkPlacementCommand cmd = mapper.readValue(commandData, ChunkPlacementCommand.class);
            switch (cmd.type()) {
                case "place" -> chunkMap.put(cmd.objectKey(), List.copyOf(cmd.nodes()));
                case "delete" -> chunkMap.remove(cmd.objectKey());
                case "rebalance" -> chunkMap.put(cmd.objectKey(), List.copyOf(cmd.nodes()));
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

    public boolean containsKey(String objectKey) {
        return chunkMap.containsKey(objectKey);
    }

    public int chunkCount() {
        return chunkMap.size();
    }

    public Map<String, List<String>> getAllPlacements() {
        return Collections.unmodifiableMap(chunkMap);
    }
}
