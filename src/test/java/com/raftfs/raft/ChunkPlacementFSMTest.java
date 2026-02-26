package com.raftfs.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChunkPlacementFSMTest {

    private ChunkPlacementFSM fsm;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        fsm = new ChunkPlacementFSM();
    }

    @Test
    void applyPlaceCommand() throws Exception {
        var cmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "place", "file1.dat", "chunk-1", List.of("node-0", "node-1", "node-2"));

        fsm.apply(mapper.writeValueAsBytes(cmd));

        List<String> nodes = fsm.getNodes("file1.dat");
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains("node-0"));
    }

    @Test
    void applyDeleteCommand() throws Exception {
        var placeCmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "place", "file1.dat", "chunk-1", List.of("node-0"));
        fsm.apply(mapper.writeValueAsBytes(placeCmd));
        assertEquals(1, fsm.chunkCount());

        var deleteCmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "delete", "file1.dat", "chunk-1", List.of());
        fsm.apply(mapper.writeValueAsBytes(deleteCmd));
        assertEquals(0, fsm.chunkCount());
    }

    @Test
    void applyRebalanceCommand() throws Exception {
        var placeCmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "place", "file1.dat", "chunk-1", List.of("node-0", "node-1"));
        fsm.apply(mapper.writeValueAsBytes(placeCmd));

        var rebalanceCmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "rebalance", "file1.dat", "chunk-1", List.of("node-1", "node-2", "node-3"));
        fsm.apply(mapper.writeValueAsBytes(rebalanceCmd));

        List<String> nodes = fsm.getNodes("file1.dat");
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains("node-3"));
    }

    @Test
    void snapshotAndRestore() throws Exception {
        var cmd = new ChunkPlacementFSM.ChunkPlacementCommand(
                "place", "file1.dat", "chunk-1", List.of("node-0"));
        fsm.apply(mapper.writeValueAsBytes(cmd));

        byte[] snapshot = fsm.snapshot();

        ChunkPlacementFSM newFsm = new ChunkPlacementFSM();
        newFsm.restore(snapshot);

        assertEquals(1, newFsm.chunkCount());
        assertFalse(newFsm.getNodes("file1.dat").isEmpty());
    }

    @Test
    void unknownKeyReturnsEmptyList() {
        assertTrue(fsm.getNodes("nonexistent").isEmpty());
    }

    @Test
    void invalidCommandDataDoesNotThrow() {
        assertDoesNotThrow(() -> fsm.apply("invalid json".getBytes()));
    }
}
