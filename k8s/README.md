# k8s Local Test Guide (Version-Pinned)

Reproducible local testing with `k3d`. All commands run from **repo root**.

> **ClickHouse** is **not** deployed inside k8s.
> It runs on an AWS instance and Flink connects to it over the network.
> See [Step 8](#8-enable-clickhouse-aws) to configure the endpoint.

## 0) Environment File

Copy the k3d env template before starting:

```bash
cp config/.env.k3d config/.env
```

Edit `config/.env` and set `CLICKHOUSE_IP` to your AWS ClickHouse endpoint.

## 1) Prerequisites (one-time install)

| Tool    | Version   |
|---------|-----------|
| kubectl | v1.35.0   |
| k3d     | v5.8.3    |
| k3s image (used by k3d) | rancher/k3s:v1.34.3-k3s1 |

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm -f kubectl

# k3d
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.8.3 bash
```

Verify:

```bash
kubectl version --client
k3d version
```

## 2) Full Clean Start

If you have a previous cluster running, tear it down first:

```bash
kubectl delete namespace taxi-pipeline --wait=false || true
k3d cluster delete taxi-pipeline || true
k3d registry delete taxi-registry || true
```

## 3) Create Cluster + Registry

```bash
bash k8s/registry/registry-setup.sh
```

Verify:

```bash
kubectl config current-context   # expect: k3d-taxi-pipeline
kubectl get nodes -o wide         # expect: 1 server + 2 agents
```

## 4) Build and Push Images

```bash
# Ingestor
docker build -t localhost:5000/taxi-ingestor:latest \
  -f services/ingestor/Dockerfile services/ingestor/
docker push localhost:5000/taxi-ingestor:latest

# Flink job
mvn -f services/flink-job/pom.xml clean package -DskipTests
docker build -t localhost:5000/taxi-flink-job:latest \
  -f services/flink-job/Dockerfile services/flink-job/
docker push localhost:5000/taxi-flink-job:latest
```

Note: manifests reference `k3d-taxi-registry:5000/...` — pushing to `localhost:5000` works because both resolve to the same local k3d registry.

## 5) Deploy Pipeline

Apply in order — each group must be ready before the next:

```bash
# Namespace
kubectl apply -f k8s/namespace.yaml

# Kafka (wait until all 3 brokers are ready)
kubectl apply -f k8s/kafka/
kubectl wait --for=condition=ready pod -l app=kafka -n taxi-pipeline --timeout=180s

# Ingestor
kubectl apply -f k8s/ingestor/
kubectl wait --for=condition=ready pod -l app=ingestor -n taxi-pipeline --timeout=180s

# Flink (has init containers that wait for Kafka + create topic automatically)
kubectl apply -f k8s/flink/
kubectl wait --for=condition=ready pod -l app=flink,component=jobmanager -n taxi-pipeline --timeout=180s
```

## 6) Smoke Test

```bash
kubectl get pods -n taxi-pipeline
curl -i http://localhost:30080/health
```

Expected:
- All pods `Running`
- Ingestor `/health` -> `200 OK`
- Kafka UI -> `http://localhost:30090`
- Flink UI -> `http://localhost:30081`

## 7) End-to-End Traffic Test (optional)

In a separate terminal:

```bash
cd services/generator
./build/generate config/k3d.yaml
```

Check logs:

```bash
kubectl logs -l app=ingestor -n taxi-pipeline --tail=50
kubectl logs -l app=flink,component=jobmanager -n taxi-pipeline --tail=50
```

## 8) Enable ClickHouse (AWS)

ClickHouse runs on AWS, **not** inside the k8s cluster.
Flink connects to it via the external endpoint you provide.

**Prerequisites:**
- A running ClickHouse instance on AWS (e.g. EC2 or ClickHouse Cloud)
- The `taxi_events` table created using `infra/clickhouse/schema.sql`
- Network access from k3d pods to the AWS host (ensure security group allows port 8123)

**Steps:**

1. Edit `k8s/flink/flink-configmap.yaml`:
   - Set `FLINK_CLICKHOUSE_HOST` to your AWS ClickHouse IP/hostname
   - Set `FLINK_ENABLE_CLICKHOUSE_SINK: "true"`
2. Re-apply and restart:

```bash
kubectl apply -f k8s/flink/flink-configmap.yaml
kubectl rollout restart deployment/flink-jobmanager -n taxi-pipeline
kubectl rollout restart deployment/flink-taskmanager -n taxi-pipeline
```

3. Verify the connection:

```bash
kubectl logs -l app=flink,component=taskmanager -n taxi-pipeline --tail=50
```

## 9) Teardown

```bash
kubectl delete namespace taxi-pipeline --wait=false || true
k3d cluster delete taxi-pipeline
k3d registry delete taxi-registry
```

To start over, go back to **Step 3**.
