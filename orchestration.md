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
- **Phase 1**: Use **k3d** (k3s in Docker) for single-machine testing — validate all k8s manifests on one Linux machine
- **Phase 1.5**: Use custom Docker image (**rancher/k3s + Tailscale**) for multi-machine — each team member runs one Docker container that joins a single k3s cluster, works on Linux/Mac/Windows uniformly
- Private **Docker registry** via k3d's built-in registry support (Phase 1) or Tailscale-accessible registry (Phase 1.5)
- Logging (Phase 2) deferred — focus on migration first
- Networking across machines via **Tailscale** (mesh VPN) — used inside containers in Phase 1.5

---

## Phase 1: K3s Migration Plan (Single-Machine via k3d)

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
│   └── registry-setup.sh           # k3d cluster + registry creation script
├── kafka/
│   ├── kafka-headless-service.yaml  # Headless for stable DNS
│   ├── kafka-statefulset.yaml       # 3-broker StatefulSet
│   ├── kafka-configmap.yaml         # Shared broker config
│   └── kafka-ui.yaml               # Kafka UI deployment + service
├── ingestor/
│   ├── ingestor-deployment.yaml     # 3 replicas
│   ├── ingestor-service.yaml        # NodePort for external Nginx
│   └── ingestor-configmap.yaml      # Kafka connection + tuning
└── flink/
    ├── flink-configmap.yaml         # Kafka + ClickHouse connection + flink-conf.yaml
    ├── flink-jobmanager-deployment.yaml
    ├── flink-jobmanager-service.yaml
    └── flink-taskmanager-deployment.yaml
```

### Step 1 — k3d Cluster + Local Registry Setup

k3d has built-in registry support — no manual `registry:2` container or `registries.yaml` needed.
Pin tool/runtime versions for reproducibility:
- `kubectl`: `v1.35.0`
- `k3d`: `v5.8.3`
- k3d cluster image: `rancher/k3s:v1.34.3-k3s1`
- optional host k3s binary: `v1.34.3+k3s1`

```bash
# Install k3d (pinned)
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.8.3 bash

# Create a local registry and k3d cluster in one go
k3d registry create taxi-registry --port 5000
k3d cluster create taxi-pipeline \
  --servers 1 --agents 2 \
  --image rancher/k3s:v1.34.3-k3s1 \
  --registry-use k3d-taxi-registry:5000 \
  -p "30080:30080@server:0" \
  -p "30081:30081@server:0" \
  -p "30090:30090@server:0"
```

- Creates a local Docker registry + a 3-node k3d cluster (1 server, 2 agents)
- `-p` flags map NodePorts to host — required for k3d (NodePorts are inside Docker, not on host by default)
  - 30080: Ingestor NodePort (external Nginx → k8s)
  - 30081: Flink dashboard NodePort
  - 30090: Kafka UI NodePort
- Images pushed to `localhost:5000/<image>:<tag>` are available to the k3d cluster registry
- Cluster lifecycle: `k3d cluster delete taxi-pipeline` for clean teardown

### Step 2 — Build & Push Docker Images

```bash
# Ingestor (multi-stage build handles gradle internally)
docker build -t localhost:5000/taxi-ingestor:latest -f services/ingestor/Dockerfile services/ingestor/
docker push localhost:5000/taxi-ingestor:latest

# Flink job (build JAR first)
cd services/flink-job && mvn clean package -DskipTests
docker build -t localhost:5000/taxi-flink-job:latest -f Dockerfile .
docker push localhost:5000/taxi-flink-job:latest
```

### Step 3 — Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: taxi-pipeline
```

### Step 4 — Kafka StatefulSet (3 brokers, KRaft)

Most complex piece — KRaft needs stable pod identities.

**Headless Service:**
- `clusterIP: None`
- Provides stable DNS: `kafka-0.kafka-headless.taxi-pipeline.svc.cluster.local`

**ConfigMap:**
- Shared config: cluster ID `MkU3OEVBNTcwNTJENDM2Qk`, replication factor (2), compression (lz4), retention (24h), partitions (4)
- `KAFKA_CONTROLLER_QUORUM_VOTERS`: `1@kafka-0.kafka-headless.taxi-pipeline.svc.cluster.local:19093,2@kafka-1.kafka-headless.taxi-pipeline.svc.cluster.local:19093,3@kafka-2.kafka-headless.taxi-pipeline.svc.cluster.local:19093`

**StatefulSet:**
- 3 replicas, image: `confluentinc/cp-kafka:7.5.0`
- **Init container** to compute per-broker config from pod ordinal:
  - `KAFKA_NODE_ID` = ordinal + 1
  - `KAFKA_ADVERTISED_LISTENERS` = `INTERNAL://kafka-{ordinal}.kafka-headless.taxi-pipeline.svc.cluster.local:29092`
- **Command wrapper**: Sources init-generated env file, then `exec /etc/confluent/docker/run`
- `podManagementPolicy: Parallel` for faster quorum formation
- Listeners: INTERNAL (29092) + CONTROLLER (19093)
  - No EXTERNAL listener needed — all consumers are in-cluster
- **PVC**: `volumeClaimTemplates` (5Gi) using k3s default `local-path` storage class
- **Readiness**: TCP 29092
- **Liveness**: exec `kafka-broker-api-versions --bootstrap-server localhost:29092`
- **Resources**: 512Mi-1Gi memory, 250m-1000m CPU

### Step 4.5 — Kafka UI Deployment

**Deployment + Service** (`k8s/kafka/kafka-ui.yaml`):
- Image: `provectuslabs/kafka-ui:latest`
- 1 replica, container port 8080
- Environment:
  - `KAFKA_CLUSTERS_0_NAME`: `taxi-pipeline`
  - `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS`: `kafka-0.kafka-headless.taxi-pipeline.svc.cluster.local:29092,kafka-1.kafka-headless.taxi-pipeline.svc.cluster.local:29092,kafka-2.kafka-headless.taxi-pipeline.svc.cluster.local:29092`
  - `DYNAMIC_CONFIG_ENABLED`: `true`
- NodePort Service: port 8080 → NodePort 30090
- Init container: waits for at least 1 Kafka broker before starting
- Useful for debugging topics, consumer groups, and message flow during migration

### Step 5 — Ingestor Deployment (3 replicas)

**ConfigMap:**
- `SPRING_KAFKA_BOOTSTRAP_SERVERS`: `kafka-0.kafka-headless.taxi-pipeline.svc.cluster.local:29092,kafka-1.kafka-headless.taxi-pipeline.svc.cluster.local:29092,kafka-2.kafka-headless.taxi-pipeline.svc.cluster.local:29092`
- `APP_KAFKA_TOPIC`: `taxi-event-data`
- Tuning: buffer 10000, batch 500, timeout 50ms, concurrency 4

**Deployment:**
- 3 replicas, container port 8080
- **Init container**: `wait-for-kafka` — polls at least 2 Kafka brokers before starting (replaces docker-compose `depends_on`)
- Image: `k3d-taxi-registry:5000/taxi-ingestor:latest`
- Readiness/Liveness: HTTP GET `/health`
- Resources: 256Mi-512Mi memory, 250m-500m CPU

**NodePort Service:**
- Port 8080 → NodePort 30080
- External Nginx routes to `localhost:30080` (single-machine) or `<node-tailscale-ip>:30080` (multi-machine)
- k8s handles load balancing across 3 pods internally

### Step 6 — Flink Deployments

**ConfigMap:**
- Kafka bootstrap servers (same as ingestor)
- ClickHouse host/port pointing to AWS endpoint
- All FLINK_* tuning params
- Embedded `flink-conf.yaml` content (mounted as volume, not FLINK_PROPERTIES env var):
  ```
  jobmanager.rpc.address: flink-jobmanager
  parallelism.default: 1
  taskmanager.numberOfTaskSlots: 2
  ```

**JobManager Deployment:**
- 1 replica
- `args: ["standalone-job", "--job-classname", "com.example.TaxiRealtimeJob"]` (uses `args` not `command` to preserve entrypoint)
- **Init container**: waits for Kafka readiness
- Mounts `flink-conf.yaml` via `subPath` to preserve other default configs
- Ports: 8081 (REST), 6123 (RPC)
- Resources: 512Mi-1Gi memory

**JobManager Service:**
- ClusterIP for RPC (6123) — TaskManager connects here
- NodePort for REST UI (8081 → 30081) — access Flink dashboard

**TaskManager Deployment:**
- 1 replica (scale to 3 later)
- `args: ["taskmanager"]`
- Same image and flink-conf.yaml mount as JobManager
- Resources: 512Mi-1.5Gi memory (higher — runs actual processing)

### Step 7 — External Nginx Config for K8s

Use the dedicated K8s nginx config `infra/nginx/docker-compose.k8s.yml` + `infra/nginx/templates/nginx.k8s.conf.template`:
```nginx
upstream ingestors {
    least_conn;
    server ${INGESTOR_1_IP}:${INGESTOR_1_PORT};
}
```
K8s handles pod-level load balancing internally via kube-proxy. Nginx only needs one NodePort entry.
Do NOT modify `nginx.distributed.conf.template` — that file serves Mode 2 (distributed w/o K8s).

### Step 8 — Deploy & Verify

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl wait --for=condition=ready pod -l app=kafka -n taxi-pipeline --timeout=120s
kubectl apply -f k8s/ingestor/
kubectl apply -f k8s/flink/
```

### Verification Checklist

- [ ] `kubectl get pods -n taxi-pipeline` — all pods Running (3 kafka, 1 kafka-ui, 3 ingestor, 2 flink)
- [ ] `kubectl logs kafka-0 -n taxi-pipeline` — broker healthy, quorum formed
- [ ] Exec into kafka pod, verify topic: `kafka-topics --list --bootstrap-server localhost:29092`
- [ ] Start generator → check ingestor logs: `kubectl logs -l app=ingestor -n taxi-pipeline`
- [ ] Port-forward Flink UI: `kubectl port-forward svc/flink-jobmanager 8081:8081 -n taxi-pipeline`
- [ ] Kafka UI: open `http://localhost:30090` — verify topics and consumer groups visible
- [ ] Query ClickHouse: `SELECT count(*) FROM taxi_events` — data flowing end-to-end

### Files to Modify

| File | Change |
|------|--------|
| `infra/nginx/templates/nginx.k8s.conf.template` (new) | Single upstream to NodePort on node IP |
| `infra/nginx/docker-compose.k8s.yml` (new) | K8s-specific compose for external Nginx |
| `services/generator/config/default.yaml` | Verify `ingestion_url` points to Nginx |

### Files to Create

All files under `k8s/` directory — 12 YAML files + 1 setup script

---

## Phase 1.5: Multi-Machine Cluster (rancher/k3s + Tailscale in Docker)

### Concept

Build one Docker image containing Linux + k3s + Tailscale. Run it on any machine (Linux/Mac/Windows with Docker). Containers join together as one real k3s cluster across machines.

**Key insight**: Tailscale runs **inside** the container, so each container gets its own mesh IP regardless of host OS. This bypasses Docker Desktop's NAT on Mac/Windows.

### Why It Works
- **`rancher/k3s` image** — proven base (k3d depends on it), handles cgroups/containerd
- **Tailscale inside container** — each container gets a unique mesh IP
- **`--flannel-iface=tailscale0`** — tells k3s to route pod traffic through the Tailscale mesh
- **Same k8s manifests** — manifests from Phase 1 work unchanged

### Architecture

```
Machine A (Linux)                    Machine B (Mac)                   Machine C (Windows)
┌─────────────────────┐             ┌─────────────────────┐           ┌─────────────────────┐
│ Docker              │             │ Docker Desktop      │           │ Docker Desktop      │
│ ┌─────────────────┐ │             │ ┌─────────────────┐ │           │ ┌─────────────────┐ │
│ │ Container       │ │             │ │ Container       │ │           │ │ Container       │ │
│ │  Linux          │ │             │ │  Linux          │ │           │ │  Linux          │ │
│ │  + Tailscale ───┼─┼── mesh ─────┼─┼── Tailscale ───┼─┼── mesh ──┼─┼── Tailscale    │ │
│ │  + k3s server   │ │             │ │  + k3s agent    │ │           │ │  + k3s agent    │ │
│ │  (controller)   │ │             │ │  (worker)       │ │           │ │  (worker)       │ │
│ └─────────────────┘ │             │ └─────────────────┘ │           │ └─────────────────┘ │
└─────────────────────┘             └─────────────────────┘           └─────────────────────┘
```

### Docker Image (`k8s/multi-node/Dockerfile`)

```
FROM rancher/k3s:v1.28.4-k3s1
+ install tailscale
+ entrypoint script:
    1. Start tailscaled
    2. Authenticate via pre-auth key (TS_AUTHKEY env var)
    3. If K3S_ROLE=server → k3s server --bind-address=<tailscale-ip> --flannel-iface=tailscale0
       If K3S_ROLE=agent  → k3s agent --server=https://<controller-tailscale-ip>:6443
```

### Usage Per Machine

```bash
# Controller (any OS with Docker):
docker run --privileged -e K3S_ROLE=server -e TS_AUTHKEY=tskey-xxx ... k3s-tailscale:latest

# Worker (any OS with Docker):
docker run --privileged -e K3S_ROLE=agent -e K3S_URL=https://<controller-ts-ip>:6443 -e TS_AUTHKEY=tskey-xxx ... k3s-tailscale:latest
```

### Files to Create

- `k8s/multi-node/Dockerfile` — rancher/k3s + tailscale
- `k8s/multi-node/entrypoint.sh` — process manager for tailscaled + k3s
- `k8s/multi-node/README.md` — setup instructions for team members

### Key Considerations

- Requires `--privileged` (k3s needs cgroup access for container management)
- Tailscale pre-auth keys expire; use reusable keys or OAuth for teams
- Private Docker registry needs to be accessible via Tailscale IP (not k3d built-in)
- k8s manifests from Phase 1 work unchanged — only cluster setup differs

---

## Phase 2: Full Pipeline Logging (TBD)

Planned stack: PLG (Promtail + Loki + Grafana) — lightweight, fits k3s well.
Details to be planned after Phase 1 is running.
