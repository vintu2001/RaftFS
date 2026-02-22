package com.raftfs.config;

import com.raftfs.raft.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class RaftConfig {

    @Value("${raft.node-id}")
    private String nodeId;

    @Value("${raft.peers}")
    private String peersString;

    @Bean
    public RaftLog raftLog() {
        return new RaftLog();
    }

    @Bean
    public ChunkPlacementFSM chunkPlacementFSM() {
        return new ChunkPlacementFSM();
    }

    @Bean
    public RaftRpcService raftRpcService() {
        return new RaftRpcService();
    }

    @Bean
    public RaftNode raftNode(RaftLog raftLog, ChunkPlacementFSM fsm, RaftRpcService rpcService) {
        List<String> peers = Arrays.stream(peersString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty() && !s.equals(nodeId))
                .collect(Collectors.toList());

        RaftNode node = new RaftNode(nodeId, peers, raftLog, fsm, rpcService);
        rpcService.setRaftNode(node);
        node.start();
        return node;
    }
}
