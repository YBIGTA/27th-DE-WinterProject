## My plan is to run this whole repo using kubernetes.

### Facts decided
I want to orchestrate 3 kafka broker, 3 ingestion server, 1 flink (which will further increased to 3)

generator + nginx load balancer will run by it self outside the cluster
click house and s3 will be ran by the aws

I want to use first phase to implement the kubenetes on my repo
Secondly I want to implement full loging on the pipeline

Im running it on various machine.
I will run the controller node via linux
and the worker node will be a mix of mac, linux, window

### Facts considering
since full k8s is heavy im leaning to run on k3s

### Decisions made
- Workers use **k3d** (k3s in Docker) — workers just need Docker Desktop, one command to join
- Private **Docker registry on the controller node**, accessible to all nodes via Tailscale IP
- Logging (Phase 2) deferred — focus on migration first
- Networking across machines via **Tailscale** (mesh VPN)

---

## Phase 1: K3s Migration Plan

### Component Placement

| Component | Location | K8s Resource | Why |
|-----------|----------|-------------|-----|
| Kafka (3 brokers) | In cluster | StatefulSet | Stable identity for KRaft |
| Ingestor (3) | In cluster | Deployment | Stateless, scalable |
| Flink JM + TM | In cluster | Deployments | Stream processing |
| Generator + Nginx | Outside cluster | N/A | Stays as-is |
| ClickHouse + S3 | AWS | N/A | Managed services |

### Directory Structure

```
k8s/
├── namespace.yaml
├── registry/
│   └── registry-setup.sh           # Script to run private registry on controller
├── kafka/
│   ├── kafka-headless-service.yaml  # Headless for stable DNS
│   ├── kafka-statefulset.yaml       # 3-broker StatefulSet
│   └── kafka-configmap.yaml         # Shared broker config
├── ingestor/
│   ├── ingestor-deployment.yaml     # 3 replicas
│   ├── ingestor-service.yaml        # NodePort for external Nginx
│   └── ingestor-configmap.yaml      # Kafka connection + tuning
└── flink/
    ├── flink-configmap.yaml         # Kafka + ClickHouse connection
    ├── flink-jobmanager-deployment.yaml
    ├── flink-jobmanager-service.yaml
    └── flink-taskmanager-deployment.yaml
```

### Step 1 — k3d Cluster Setup + Tailscale Networking

**Controller node (Linux machine):**
- Install k3d: `curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash`
- Create cluster: `k3d cluster create taxi-pipeline --servers 1 --agents 0`
- k3s API server listens on the Tailscale IP so workers can join

**Worker nodes (other people's machines — Mac/Win/Linux):**
- Prerequisite: Docker Desktop installed + Tailscale connected
- Join script: single command using k3d agent pointing to controller's Tailscale IP
- No Linux VM or WSL2 needed — k3d handles everything inside Docker

### Step 2 — Private Docker Registry on Controller

```bash
docker run -d -p 5000:5000 --restart always --name registry registry:2
```

- All nodes access it at `<controller-tailscale-ip>:5000`
- Configure k3d to trust this registry (insecure registry in registries.yaml)
- Images tagged as `<controller-tailscale-ip>:5000/taxi-ingestor:latest`, etc.

### Step 3 — Build & Push Docker Images

```bash
# Ingestor
docker build -t <TAILSCALE_IP>:5000/taxi-ingestor:latest -f services/ingestor/Dockerfile services/ingestor/
docker push <TAILSCALE_IP>:5000/taxi-ingestor:latest

# Flink job (build JAR first)
cd services/flink-job && mvn clean package -DskipTests
docker build -t <TAILSCALE_IP>:5000/taxi-flink-job:latest -f Dockerfile .
docker push <TAILSCALE_IP>:5000/taxi-flink-job:latest
```

### Step 4 — Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: taxi-pipeline
```

### Step 5 — Kafka StatefulSet (3 brokers, KRaft)

Most complex piece — KRaft needs stable pod identities.

**Headless Service:**
- `clusterIP: None`
- Provides stable DNS: `kafka-0.kafka-headless.taxi-pipeline.svc.cluster.local`

**ConfigMap:**
- Shared config: cluster ID, replication factor (2), compression (lz4), retention (24h), partitions (4)
- `KAFKA_CONTROLLER_QUORUM_VOTERS`: `1@kafka-0.kafka-headless:19093,2@kafka-1.kafka-headless:19093,3@kafka-2.kafka-headless:19093`

**StatefulSet:**
- 3 replicas, image: `confluentinc/cp-kafka:7.5.0`
- **Init container** to compute per-broker config from pod ordinal:
  - `KAFKA_NODE_ID` = ordinal + 1
  - `KAFKA_ADVERTISED_LISTENERS` = `INTERNAL://kafka-{ordinal}.kafka-headless:29092`
- Listeners: INTERNAL (29092) + CONTROLLER (19093)
  - No EXTERNAL listener needed — all consumers are in-cluster
- **PVC**: `volumeClaimTemplates` using k3s default `local-path` storage class
- **Readiness**: TCP 29092
- **Liveness**: exec `kafka-broker-api-versions`

### Step 6 — Ingestor Deployment (3 replicas)

**ConfigMap:**
- `SPRING_KAFKA_BOOTSTRAP_SERVERS`: `kafka-0.kafka-headless:29092,kafka-1.kafka-headless:29092,kafka-2.kafka-headless:29092`
- `APP_KAFKA_TOPIC`: `taxi-event-data`
- Tuning: buffer 10000, batch 500, timeout 50ms, concurrency 4

**Deployment:**
- 3 replicas, container port 8080
- Readiness/Liveness: HTTP GET `/health`
- Resources: 256Mi-512Mi memory, 250m-500m CPU

**NodePort Service:**
- Port 8080 → NodePort 30080
- External Nginx routes to `<any-node-tailscale-ip>:30080`
- k8s handles load balancing across 3 pods internally

### Step 7 — Flink Deployments

**ConfigMap:**
- Kafka bootstrap servers (same as ingestor)
- ClickHouse host/port pointing to AWS endpoint
- All FLINK_* tuning params

**JobManager Deployment:**
- 1 replica, command: `standalone-job --job-classname com.example.TaxiRealtimeJob`
- Ports: 8081 (REST), 6123 (RPC)

**JobManager Service:**
- ClusterIP for RPC (6123) — TaskManager connects here
- NodePort for REST UI (8081 → 30081) — access Flink dashboard via Tailscale

**TaskManager Deployment:**
- 1 replica (scale to 3 later), command: `taskmanager`
- `JOB_MANAGER_RPC_ADDRESS`: `flink-jobmanager`

### Step 8 — Update External Nginx Config

Modify `infra/nginx/templates/nginx.distributed.conf.template`:
```nginx
upstream ingestors {
    least_conn;
    server <node-tailscale-ip>:30080;
}
```

### Step 9 — Deploy & Verify

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
# Wait for kafka pods to be ready
kubectl apply -f k8s/ingestor/
kubectl apply -f k8s/flink/
```

### Verification Checklist

- [ ] `kubectl get pods -n taxi-pipeline` — all pods Running (3 kafka, 3 ingestor, 2 flink)
- [ ] `kubectl logs kafka-0 -n taxi-pipeline` — broker healthy, quorum formed
- [ ] Exec into kafka pod, verify topic: `kafka-topics --list --bootstrap-server localhost:29092`
- [ ] Start generator → check ingestor logs: `kubectl logs -l app=ingestor -n taxi-pipeline`
- [ ] Port-forward Flink UI: `kubectl port-forward svc/flink-jobmanager 8081:8081 -n taxi-pipeline`
- [ ] Query ClickHouse: `SELECT count(*) FROM taxi_events` — data flowing end-to-end

### Files to Modify

| File | Change |
|------|--------|
| `infra/nginx/templates/nginx.distributed.conf.template` | Update upstream to NodePort on Tailscale IP |
| `services/generator/config/default.yaml` | Verify `ingestion_url` points to Nginx |

### Files to Create

All files under `k8s/` directory — 11 YAML files + 1 setup script

---

## Phase 2: Full Pipeline Logging (TBD)

Planned stack: PLG (Promtail + Loki + Grafana) — lightweight, fits k3s well.
Details to be planned after Phase 1 is running.
