# k8s Local Test Guide (Version-Pinned)

This document pins tool versions for reproducible local testing with `k3d`.

## 1) Version Pin Policy

- `kubectl`: `v1.35.0`
- `k3d`: `v5.8.3`
- `k3s image used by k3d cluster`: `rancher/k3s:v1.34.3-k3s1`
- `k3s host binary (optional install)`: `v1.34.3+k3s1`

Why:
- `kubectl` should stay within supported version skew relative to cluster API server.
- k3d default image can change over time, so cluster version must be pinned explicitly.

## 2) Install (Pinned)

### 2.1 kubectl v1.35.0 (Linux x86_64)

```bash
curl -LO "https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm -f kubectl
```

### 2.2 k3d v5.8.3

```bash
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.8.3 bash
```

### 2.3 k3s host binary (optional for this repo)

For this repo's Phase 1 tests, k3s runs inside Docker via `k3d`.
Host `k3s` install is optional and not required to create/test the cluster.

If you install it anyway, pin the version:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.34.3+k3s1 sh -
```

Note:
- host binary version uses `+k3s1`
- container image tag uses `-k3s1` (`rancher/k3s:v1.34.3-k3s1`)

## 3) Verify Installed Versions

```bash
kubectl version --client
k3d version
k3s --version || true
```

Expected:
- `kubectl` client `v1.35.0`
- `k3d` `v5.8.3`
- `k3s` host binary `v1.34.3+k3s1` (if installed)
- `k3d` default k3s shown by `k3d version` may differ; cluster creation script forces pinned image

## 4) Create Cluster with Pinned k3s Image

`k8s/registry/registry-setup.sh` is version-pinned by default:
- `K3S_IMAGE=rancher/k3s:v1.34.3-k3s1`

Run:

```bash
bash k8s/registry/registry-setup.sh
```

Optional override:

```bash
K3S_IMAGE=rancher/k3s:v1.34.3-k3s1 bash k8s/registry/registry-setup.sh
```

## 5) Quick Sanity Checks

```bash
kubectl config current-context
kubectl get nodes -o wide
kubectl version --short
```

If cluster is created by script, context name should be:
- `k3d-taxi-pipeline`

## 6) Next Step (Run the Pipeline Test)

This repo is configured to start with ClickHouse sink disabled for initial smoke tests:
- `k8s/flink/flink-configmap.yaml` -> `FLINK_ENABLE_CLICKHOUSE_SINK: "false"`

### 6.1 Build and Push App Images

```bash
# run at repo root
docker build -t localhost:5000/taxi-ingestor:latest -f services/ingestor/Dockerfile services/ingestor/
docker push localhost:5000/taxi-ingestor:latest

cd services/flink-job
mvn clean package -DskipTests
docker build -t localhost:5000/taxi-flink-job:latest -f Dockerfile .
docker push localhost:5000/taxi-flink-job:latest
cd ../..
```

Note:
- manifests reference `k3d-taxi-registry:5000/...`
- pushing to `localhost:5000/...` is valid because both point to the same local k3d registry

### 6.2 Apply Manifests

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl wait --for=condition=ready pod -l app=kafka -n taxi-pipeline --timeout=180s

kubectl apply -f k8s/ingestor/
kubectl wait --for=condition=ready pod -l app=ingestor -n taxi-pipeline --timeout=180s

kubectl apply -f k8s/flink/
kubectl wait --for=condition=ready pod -l app=flink,component=jobmanager -n taxi-pipeline --timeout=180s
```

### 6.3 Smoke Test

```bash
kubectl get pods -n taxi-pipeline
curl -i http://localhost:30080/health
```

Expected:
- ingestor `/health` -> `200 OK`
- Kafka UI -> `http://localhost:30090`
- Flink UI -> `http://localhost:30081`

### 6.4 Optional End-to-End Traffic Test (Generator)

```bash
# run in another terminal, repo root
export INGEST_URL="http://localhost:30080/ingest"
./services/generator/build/generate ./services/generator/config/default.yaml
```

Then check logs:

```bash
kubectl logs -l app=ingestor -n taxi-pipeline --tail=200
kubectl logs -l app=flink,component=jobmanager -n taxi-pipeline --tail=200
```

## 7) Enable ClickHouse Later

When AWS ClickHouse endpoint is ready:

1. Set `FLINK_CLICKHOUSE_HOST` in `k8s/flink/flink-configmap.yaml`
2. Set `FLINK_ENABLE_CLICKHOUSE_SINK: "true"`
3. Re-apply and restart Flink pods:

```bash
kubectl apply -f k8s/flink/flink-configmap.yaml
kubectl rollout restart deployment/flink-jobmanager -n taxi-pipeline
kubectl rollout restart deployment/flink-taskmanager -n taxi-pipeline
```

## 8) Cleanup

```bash
kubectl delete namespace taxi-pipeline --wait=false || true
k3d cluster delete taxi-pipeline
k3d registry delete taxi-registry
```
