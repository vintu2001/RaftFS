#!/bin/bash
set -euo pipefail

PROJECT_ID=${1:?"Usage: $0 <gcp-project-id>"}
CLUSTER_NAME=${2:-"raftfs-cluster"}
ZONE=${3:-"us-central1-a"}

echo "Creating GKE cluster: $CLUSTER_NAME"
gcloud container clusters create "$CLUSTER_NAME" \
  --project "$PROJECT_ID" \
  --zone "$ZONE" \
  --num-nodes 3 \
  --machine-type e2-standard-2 \
  --enable-ip-alias \
  --workload-pool="${PROJECT_ID}.svc.id.goog"

echo "Creating GCS buckets for each storage node"
for i in 0 1 2; do
  gsutil mb -p "$PROJECT_ID" -l us-central1 "gs://raftfs-chunks-storage-node-${i}/" || true
done

echo "Creating Kubernetes service account"
kubectl create serviceaccount raftfs-gcs-sa || true

echo "Creating GCP service account for GCS access"
gcloud iam service-accounts create raftfs-gcs-sa \
  --project "$PROJECT_ID" \
  --display-name "RaftFS GCS Service Account" || true

echo "Granting GCS access to service account"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member "serviceAccount:raftfs-gcs-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role "roles/storage.admin"

echo "Binding Kubernetes SA to GCP SA via Workload Identity"
gcloud iam service-accounts add-iam-policy-binding \
  "raftfs-gcs-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[default/raftfs-gcs-sa]"

kubectl annotate serviceaccount raftfs-gcs-sa \
  "iam.gke.io/gcp-service-account=raftfs-gcs-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --overwrite

echo "Updating configmap with project ID"
sed -i.bak "s/your-gcp-project-id/$PROJECT_ID/g" k8s/configmap.yaml

echo "Building and pushing Docker image"
docker build -t "gcr.io/${PROJECT_ID}/raftfs-storage-node:latest" .
docker push "gcr.io/${PROJECT_ID}/raftfs-storage-node:latest"

echo "Updating StatefulSet image reference"
sed -i.bak "s/PROJECT_ID/$PROJECT_ID/g" k8s/statefulset.yaml

echo "Deploying to GKE"
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/headless-service.yaml
kubectl apply -f k8s/statefulset.yaml

echo "Waiting for pods to be ready"
kubectl rollout status statefulset/storage-node --timeout=120s

echo "Cluster bootstrap complete"
kubectl get pods -l app=storage-node
