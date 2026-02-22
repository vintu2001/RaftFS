package com.raftfs.config;

import com.raftfs.raft.ChunkPlacementFSM;
import com.raftfs.raft.RaftNode;
import com.raftfs.ring.ConsistentHashRing;
import com.raftfs.server.StorageGrpcService;
import com.raftfs.storage.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class StorageConfig {

    @Value("${storage.data-dir:/data}")
    private String dataDir;

    @Value("${storage.gcs.bucket:}")
    private String gcsBucket;

    @Value("${storage.gcs.project-id:}")
    private String gcsProjectId;

    @Value("${storage.local-address}")
    private String localAddress;

    @Value("${raft.peers}")
    private String peersString;

    @Bean
    public ChunkStore chunkStore() {
        if (gcsBucket != null && !gcsBucket.isEmpty()) {
            return new GcsChunkStore(gcsBucket, gcsProjectId);
        }
        return new LocalChunkStore(dataDir);
    }

    @Bean
    public ConsistentHashRing consistentHashRing() {
        ConsistentHashRing ring = new ConsistentHashRing();
        Arrays.stream(peersString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(ring::addNode);
        return ring;
    }

    @Bean
    public ReplicationManager replicationManager(ConsistentHashRing ring, ChunkStore chunkStore) {
        return new ReplicationManager(ring, chunkStore, localAddress);
    }

    @Bean
    public StorageGrpcService storageGrpcService(ReplicationManager replicationManager,
                                                  RaftNode raftNode,
                                                  ChunkPlacementFSM fsm,
                                                  ChunkStore chunkStore,
                                                  ConsistentHashRing ring) {
        return new StorageGrpcService(replicationManager, raftNode, fsm, chunkStore, ring);
    }
}
