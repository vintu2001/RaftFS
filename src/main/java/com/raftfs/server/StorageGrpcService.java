package com.raftfs.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftfs.proto.*;
import com.raftfs.raft.ChunkPlacementFSM;
import com.raftfs.raft.RaftNode;
import com.raftfs.ring.ConsistentHashRing;
import com.raftfs.storage.ChunkStore;
import com.raftfs.storage.ReplicationManager;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StorageGrpcService extends ObjectStorageServiceGrpc.ObjectStorageServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(StorageGrpcService.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ReplicationManager replicationManager;
    private final RaftNode raftNode;
    private final ChunkPlacementFSM fsm;
    private final ChunkStore localStore;
    private final ConsistentHashRing ring;

    public StorageGrpcService(ReplicationManager replicationManager, RaftNode raftNode,
                              ChunkPlacementFSM fsm, ChunkStore localStore,
                              ConsistentHashRing ring) {
        this.replicationManager = replicationManager;
        this.raftNode = raftNode;
        this.fsm = fsm;
        this.localStore = localStore;
        this.ring = ring;
    }

    @Override
    public void putObject(PutObjectRequest request, StreamObserver<PutObjectResponse> responseObserver) {
        String key = request.getKey();
        byte[] data = request.getData().toByteArray();

        try {
            boolean written = replicationManager.putObject(key, data);
            if (!written) {
                responseObserver.onNext(PutObjectResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Quorum write failed")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            List<String> replicaNodes = ring.getReplicaNodes(key, 3);
            var cmd = new ChunkPlacementFSM.ChunkPlacementCommand("place", key, key, replicaNodes);
            byte[] cmdBytes = mapper.writeValueAsBytes(cmd);
            raftNode.propose(cmdBytes);

            responseObserver.onNext(PutObjectResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Object stored successfully")
                    .build());
        } catch (Exception e) {
            log.error("PutObject failed for key {}: {}", key, e.getMessage());
            responseObserver.onNext(PutObjectResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getObject(GetObjectRequest request, StreamObserver<GetObjectResponse> responseObserver) {
        String key = request.getKey();

        try {
            var data = replicationManager.getObject(key);
            if (data.isPresent()) {
                responseObserver.onNext(GetObjectResponse.newBuilder()
                        .setFound(true)
                        .setData(com.google.protobuf.ByteString.copyFrom(data.get()))
                        .build());
            } else {
                responseObserver.onNext(GetObjectResponse.newBuilder()
                        .setFound(false)
                        .build());
            }
        } catch (Exception e) {
            log.error("GetObject failed for key {}: {}", key, e.getMessage());
            responseObserver.onNext(GetObjectResponse.newBuilder()
                    .setFound(false)
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void deleteObject(DeleteObjectRequest request, StreamObserver<DeleteObjectResponse> responseObserver) {
        String key = request.getKey();

        try {
            boolean deleted = replicationManager.deleteObject(key);
            if (deleted) {
                var cmd = new ChunkPlacementFSM.ChunkPlacementCommand("delete", key, key, List.of());
                byte[] cmdBytes = mapper.writeValueAsBytes(cmd);
                raftNode.propose(cmdBytes);
            }

            responseObserver.onNext(DeleteObjectResponse.newBuilder()
                    .setSuccess(deleted)
                    .build());
        } catch (Exception e) {
            log.error("DeleteObject failed for key {}: {}", key, e.getMessage());
            responseObserver.onNext(DeleteObjectResponse.newBuilder()
                    .setSuccess(false)
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void writeChunk(WriteChunkRequest request, StreamObserver<WriteChunkResponse> responseObserver) {
        boolean success = localStore.write(request.getKey(), request.getData().toByteArray());
        responseObserver.onNext(WriteChunkResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
    }

    @Override
    public void readChunk(ReadChunkRequest request, StreamObserver<ReadChunkResponse> responseObserver) {
        byte[] data = localStore.read(request.getKey());
        if (data != null) {
            responseObserver.onNext(ReadChunkResponse.newBuilder()
                    .setFound(true)
                    .setData(com.google.protobuf.ByteString.copyFrom(data))
                    .build());
        } else {
            responseObserver.onNext(ReadChunkResponse.newBuilder().setFound(false).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void deleteChunk(DeleteChunkRequest request, StreamObserver<DeleteChunkResponse> responseObserver) {
        boolean success = localStore.delete(request.getKey());
        responseObserver.onNext(DeleteChunkResponse.newBuilder().setSuccess(success).build());
        responseObserver.onCompleted();
    }
}
