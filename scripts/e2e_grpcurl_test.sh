#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAVA_HOME_17="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
JAVA_BIN="$JAVA_HOME_17/bin/java"
JAR_PATH="$ROOT_DIR/target/raftfs-1.0.0.jar"
TMP_BASE="/tmp/raftfs"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1"
    exit 1
  }
}

require_cmd mvn
require_cmd grpcurl
require_cmd lsof

if [[ ! -x "$JAVA_BIN" ]]; then
  echo "Java 17 not found at $JAVA_BIN"
  exit 1
fi

stop_cluster() {
  pkill -9 -f "raftfs-1.0.0.jar" >/dev/null 2>&1 || true
}

wait_for_port() {
  local port="$1"
  local retries=60
  while (( retries > 0 )); do
    if lsof -i ":$port" 2>/dev/null | grep -q LISTEN; then
      return 0
    fi
    sleep 0.5
    retries=$((retries - 1))
  done
  return 1
}

start_node() {
  local node_id="$1"
  local grpc_port="$2"
  local raft_port="$3"
  local log_file="$TMP_BASE/$node_id.log"

  NODE_ID="$node_id" \
  PEERS="localhost:8085,localhost:8086,localhost:8087" \
  STORAGE_PEERS="localhost:8080,localhost:8082,localhost:8084" \
  GRPC_PORT="$grpc_port" \
  RAFT_PORT="$raft_port" \
  LOCAL_ADDRESS="localhost:$grpc_port" \
  DATA_DIR="$TMP_BASE/$node_id" \
  GCS_BUCKET="" \
  GCS_PROJECT_ID="" \
  "$JAVA_BIN" -jar "$JAR_PATH" >"$log_file" 2>&1 &

  echo "Started $node_id on grpc=$grpc_port raft=$raft_port pid=$!"
}

echo "[1/5] Building with Java 17"
cd "$ROOT_DIR"
JAVA_HOME="$JAVA_HOME_17" mvn -q clean package -DskipTests

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Build failed: missing $JAR_PATH"
  exit 1
fi

echo "[2/5] Starting clean 3-node cluster"
stop_cluster
mkdir -p "$TMP_BASE/node-0" "$TMP_BASE/node-1" "$TMP_BASE/node-2"

start_node node-0 8080 8085
start_node node-1 8082 8086
start_node node-2 8084 8087

wait_for_port 8080 || { echo "node-0 did not come up"; exit 1; }
wait_for_port 8082 || { echo "node-1 did not come up"; exit 1; }
wait_for_port 8084 || { echo "node-2 did not come up"; exit 1; }

echo "[3/5] Baseline service and object tests"
grpcurl -plaintext localhost:8080 list >/dev/null
grpcurl -plaintext localhost:8082 list >/dev/null
grpcurl -plaintext localhost:8084 list >/dev/null

grpcurl -plaintext -d '{"key":"e2e-key","data":"aGVsbG8td29ybGQ="}' \
  localhost:8080 com.raftfs.proto.ObjectStorageService/PutObject | grep -q '"success": true'

grpcurl -plaintext -d '{"key":"e2e-key"}' \
  localhost:8082 com.raftfs.proto.ObjectStorageService/GetObject | grep -q '"found": true'

grpcurl -plaintext -d '{"key":"e2e-key"}' \
  localhost:8080 com.raftfs.proto.ObjectStorageService/DeleteObject | grep -q '"success": true'

echo "[4/5] Chunk RPC tests"
grpcurl -plaintext -d '{"key":"chunk-e2e","data":"Y2h1bms="}' \
  localhost:8080 com.raftfs.proto.ObjectStorageService/WriteChunk | grep -q '"success": true'

grpcurl -plaintext -d '{"key":"chunk-e2e"}' \
  localhost:8080 com.raftfs.proto.ObjectStorageService/ReadChunk | grep -q '"found": true'

grpcurl -plaintext -d '{"key":"chunk-e2e"}' \
  localhost:8080 com.raftfs.proto.ObjectStorageService/DeleteChunk | grep -q '"success": true'

echo "[5/5] One-node-down quorum validation"
NODE0_PID="$(lsof -ti :8080 | head -1)"
if [[ -z "$NODE0_PID" ]]; then
  echo "Unable to identify node-0 PID"
  exit 1
fi
kill -9 "$NODE0_PID"
sleep 2

# Validate write succeeds via surviving nodes (2/3 quorum)
grpcurl -plaintext -d '{"key":"degraded-e2e","data":"ZGVncmFkZWQ="}' \
  localhost:8082 com.raftfs.proto.ObjectStorageService/PutObject | grep -q '"success": true'

grpcurl -plaintext -d '{"key":"degraded-e2e"}' \
  localhost:8084 com.raftfs.proto.ObjectStorageService/GetObject | grep -q '"found": true'

echo "PASS: End-to-end gRPC validation succeeded"
