package com.raftfs.client;

import com.raftfs.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class StorageGrpcClient {

    private final ManagedChannel channel;
    private final ObjectStorageServiceGrpc.ObjectStorageServiceBlockingStub stub;

    public StorageGrpcClient(String target) {
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        this.stub = ObjectStorageServiceGrpc.newBlockingStub(channel);
    }

    public StorageGrpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = ObjectStorageServiceGrpc.newBlockingStub(channel);
    }

    public boolean writeChunk(String key, byte[] data) {
        WriteChunkResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .writeChunk(WriteChunkRequest.newBuilder()
                        .setKey(key)
                        .setData(com.google.protobuf.ByteString.copyFrom(data))
                        .build());
        return response.getSuccess();
    }

    public byte[] readChunk(String key) {
        ReadChunkResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .readChunk(ReadChunkRequest.newBuilder()
                        .setKey(key)
                        .build());
        return response.getFound() ? response.getData().toByteArray() : null;
    }

    public boolean deleteChunk(String key) {
        DeleteChunkResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .deleteChunk(DeleteChunkRequest.newBuilder()
                        .setKey(key)
                        .build());
        return response.getSuccess();
    }

    public void shutdown() {
        channel.shutdownNow();
    }
}
