package com.raftfs.server;

import com.raftfs.raft.RaftRpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GrpcServerRunner {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerRunner.class);

    private final StorageGrpcService storageService;
    private final RaftRpcService raftService;

    @Value("${grpc.port:8080}")
    private int grpcPort;

    @Value("${raft.port:8081}")
    private int raftPort;

    private Server grpcServer;
    private Server raftServer;

    public GrpcServerRunner(StorageGrpcService storageService, RaftRpcService raftService) {
        this.storageService = storageService;
        this.raftService = raftService;
    }

    @PostConstruct
    public void start() throws Exception {
        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(storageService)
                .build()
                .start();
        log.info("gRPC storage server started on port {}", grpcPort);

        raftServer = ServerBuilder.forPort(raftPort)
                .addService(raftService)
                .build()
                .start();
        log.info("Raft RPC server started on port {}", raftPort);
    }

    @PreDestroy
    public void stop() {
        if (grpcServer != null) grpcServer.shutdownNow();
        if (raftServer != null) raftServer.shutdownNow();
        log.info("gRPC servers stopped");
    }
}
