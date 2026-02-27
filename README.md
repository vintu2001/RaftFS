# RaftFS - Fault-Tolerant Distributed Object Storage

A production-grade, fault-tolerant distributed object storage system built with Java 17, using Raft consensus for cluster metadata and chunk placement decisions. Deployed on Google Kubernetes Engine (GKE) with Google Cloud Storage (GCS) for durable cross-zone storage.

## Architecture

- **Raft Consensus (Control Plane)** - Manages cluster membership and maintains an authoritative chunk placement map replicated across all nodes
- **Consistent Hashing (Data Plane)** - Murmur3-based hash ring for client-side O(1) node lookup without leader involvement
- **Quorum Replication (RF=3)** - Every write requires acknowledgment from 2 of 3 replicas before success
- **GCS-backed Storage** - Each replica node persists chunk data to a dedicated GCS bucket for cross-zone durability
- **GKE Deployment** - StatefulSets provide stable network identities and persistent storage for Raft log

## Project Structure

```
src/main/java/com/raftfs/
├── raft/                        # Raft consensus implementation
│   ├── RaftNode.java            # Core state machine (leader election, log replication)
│   ├── RaftLog.java             # Persistent log with compaction
│   ├── RaftRpcService.java      # gRPC transport for Raft RPCs
│   ├── ChunkPlacementFSM.java   # Finite state machine for chunk placement
│   └── ElectionTimer.java       # Randomized election timeout
├── ring/
│   └── ConsistentHashRing.java  # Murmur3-based consistent hashing
├── storage/
│   ├── ChunkStore.java          # Storage backend interface
│   ├── GcsChunkStore.java       # Google Cloud Storage implementation
│   ├── LocalChunkStore.java     # Local disk implementation
│   ├── ReplicationManager.java  # RF=3 quorum write/read logic
│   └── ChecksumValidator.java   # CRC32C data integrity validation
├── server/
│   ├── StorageGrpcService.java  # Object storage gRPC handlers
│   └── GrpcServerRunner.java    # gRPC server lifecycle
└── client/
    ├── StorageClient.java       # SDK with hash ring routing and retry
    └── StorageGrpcClient.java   # Inter-node gRPC client
```

## Build and Run

### Prerequisites
- Java 17+
- Maven 3.8+
- Docker
- gcloud CLI (for GKE deployment)

### Local Development
```bash
mvn clean compile
mvn test
mvn spring-boot:run
```

### GKE Deployment
```bash
chmod +x scripts/cluster_bootstrap.sh
./scripts/cluster_bootstrap.sh <gcp-project-id>
```

## Design Decisions

1. **Raft for Control Plane, Consistent Hashing for Data Plane** - Raft ensures strong consistency for the chunk placement map. Consistent hashing enables O(1) client-side routing without querying the leader for every read.

2. **GCS as Durable Backend** - Each storage node persists chunks to a dedicated GCS bucket, providing cross-zone durability independent of local disk failures.

3. **Quorum Writes (2/3)** - Requires majority acknowledgment for writes. Any two quorums overlap by at least one node, guaranteeing read-after-write consistency.

4. **StatefulSets for Stable Identity** - Kubernetes StatefulSets provide stable DNS names required by Raft for peer addressing and ordered deployment for cluster bootstrap.

## Configuration

| Environment Variable | Default        | Description                              |
|---------------------|----------------|------------------------------------------|
| NODE_ID             | node-0         | Unique identifier for this Raft node     |
| PEERS               | localhost:8081 | Comma-separated list of peer addresses   |
| GRPC_PORT           | 8080           | Port for object storage gRPC service     |
| RAFT_PORT           | 8081           | Port for Raft consensus RPCs             |
| DATA_DIR            | /data          | Local data directory                     |
| GCS_BUCKET          |                | GCS bucket name (enables GCS backend)    |
| GCS_PROJECT_ID      |                | Google Cloud project ID                  |

## Technology Stack

- **Java 17** - Records, sealed classes, pattern matching
- **Raft Consensus** - Custom implementation for leader election and log replication
- **gRPC + Protobuf** - Inter-node and client-server communication
- **Google Cloud Storage** - Durable object persistence
- **Google Kubernetes Engine** - Container orchestration with StatefulSets
- **Docker** - Multi-stage build for minimal production images
- **Spring Boot 3.2** - Application framework and dependency injection
