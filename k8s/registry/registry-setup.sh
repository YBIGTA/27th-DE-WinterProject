#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-taxi-pipeline}"
REGISTRY_NAME="${REGISTRY_NAME:-taxi-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.34.3-k3s1}"

NODEPORT_INGESTOR="${NODEPORT_INGESTOR:-30080}"
NODEPORT_FLINK_UI="${NODEPORT_FLINK_UI:-30081}"
NODEPORT_KAFKA_UI="${NODEPORT_KAFKA_UI:-30090}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command '$1' is not installed." >&2
    exit 1
  fi
}

exists_in_list() {
  local name="$1"
  shift
  "$@" | awk 'NR>1 {print $1}' | grep -Fxq "$name"
}

require_cmd docker
require_cmd k3d

if exists_in_list "$REGISTRY_NAME" k3d registry list; then
  echo "Registry already exists: $REGISTRY_NAME"
else
  echo "Creating registry: $REGISTRY_NAME (port $REGISTRY_PORT)"
  k3d registry create "$REGISTRY_NAME" --port "$REGISTRY_PORT"
fi

K3D_REGISTRY_HOST="k3d-${REGISTRY_NAME}:${REGISTRY_PORT}"

if exists_in_list "$CLUSTER_NAME" k3d cluster list; then
  echo "Cluster already exists: $CLUSTER_NAME"
  echo "Skip cluster creation. If you need a clean cluster, run:"
  echo "  k3d cluster delete $CLUSTER_NAME"
  exit 0
fi

echo "Creating cluster: $CLUSTER_NAME"
k3d cluster create "$CLUSTER_NAME" \
  --servers 1 \
  --agents 2 \
  --image "$K3S_IMAGE" \
  --registry-use "$K3D_REGISTRY_HOST" \
  -p "${NODEPORT_INGESTOR}:${NODEPORT_INGESTOR}@server:0" \
  -p "${NODEPORT_FLINK_UI}:${NODEPORT_FLINK_UI}@server:0" \
  -p "${NODEPORT_KAFKA_UI}:${NODEPORT_KAFKA_UI}@server:0"

echo "Done."
echo "Local registry: $K3D_REGISTRY_HOST"
echo "Cluster: $CLUSTER_NAME"
echo "K3s image: $K3S_IMAGE"
