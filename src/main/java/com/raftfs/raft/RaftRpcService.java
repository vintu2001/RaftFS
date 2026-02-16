package com.raftfs.raft;

import com.raftfs.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RaftRpcService extends RaftServiceGrpc.RaftServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RaftRpcService.class);
    private static final int RPC_TIMEOUT_MS = 500;

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private RaftNode raftNode;

    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public record AppendEntriesResult(long term, boolean success) {}

    public boolean sendRequestVote(String peer, long term, String candidateId,
                                   long lastLogIndex, long lastLogTerm) {
        try {
            RaftServiceGrpc.RaftServiceBlockingStub stub =
                    RaftServiceGrpc.newBlockingStub(getChannel(peer))
                            .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setTerm(term)
                    .setCandidateId(candidateId)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogTerm)
                    .build();

            RequestVoteResponse response = stub.requestVote(request);
            return response.getVoteGranted();
        } catch (Exception e) {
            log.warn("RequestVote RPC to {} failed: {}", peer, e.getMessage());
            return false;
        }
    }

    public AppendEntriesResult sendAppendEntries(String peer, long term, String leaderId,
                                                  long prevLogIndex, long prevLogTerm,
                                                  List<LogEntry> entries, long leaderCommit) {
        try {
            RaftServiceGrpc.RaftServiceBlockingStub stub =
                    RaftServiceGrpc.newBlockingStub(getChannel(peer))
                            .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<LogEntryProto> protoEntries = entries.stream()
                    .map(e -> LogEntryProto.newBuilder()
                            .setTerm(e.term())
                            .setIndex(e.index())
                            .setCommand(com.google.protobuf.ByteString.copyFrom(e.command()))
                            .build())
                    .collect(Collectors.toList());

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId(leaderId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(protoEntries)
                    .setLeaderCommit(leaderCommit)
                    .build();

            AppendEntriesResponse response = stub.appendEntries(request);
            return new AppendEntriesResult(response.getTerm(), response.getSuccess());
        } catch (Exception e) {
            log.warn("AppendEntries RPC to {} failed: {}", peer, e.getMessage());
            return new AppendEntriesResult(0, false);
        }
    }

    @Override
    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> responseObserver) {
        RaftNode.RequestVoteReply reply = raftNode.handleRequestVote(
                request.getTerm(), request.getCandidateId(),
                request.getLastLogIndex(), request.getLastLogTerm());

        responseObserver.onNext(RequestVoteResponse.newBuilder()
                .setTerm(reply.term())
                .setVoteGranted(reply.granted())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {
        List<LogEntry> entries = request.getEntriesList().stream()
                .map(e -> new LogEntry(e.getTerm(), e.getIndex(), e.getCommand().toByteArray()))
                .collect(Collectors.toList());

        RaftNode.AppendEntriesReply reply = raftNode.handleAppendEntries(
                request.getTerm(), request.getLeaderId(),
                request.getPrevLogIndex(), request.getPrevLogTerm(),
                entries, request.getLeaderCommit());

        responseObserver.onNext(AppendEntriesResponse.newBuilder()
                .setTerm(reply.term())
                .setSuccess(reply.success())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request,
                                StreamObserver<InstallSnapshotResponse> responseObserver) {
        responseObserver.onNext(InstallSnapshotResponse.newBuilder()
                .setTerm(raftNode.getCurrentTerm())
                .build());
        responseObserver.onCompleted();
    }

    private ManagedChannel getChannel(String peer) {
        return channels.computeIfAbsent(peer, addr ->
                ManagedChannelBuilder.forTarget(addr)
                        .usePlaintext()
                        .build());
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
