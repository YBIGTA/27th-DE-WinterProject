# Implementation Orchestration

Downstream execution plan from `orchestration.md`.

## 1) Relationship and Rules

- Upstream source of truth: `orchestration.md`
- This file is the execution tracker for actual implementation work.
- If this file conflicts with `orchestration.md`, upstream wins.
- Update this file immediately after each meaningful implementation change.
- Existing file overwrite is prohibited by default.
- If modifying an existing file is unavoidable, align scope and intent first before editing.

## 2) Current Scope

- Active phase: `Phase 1.5 (Multi-machine via k3s + Tailscale in Docker)`
- Goal: build and validate cross-machine k3s cluster runtime while reusing existing `k8s/` manifests from Phase 1

## 3) Status Legend

- `TODO`: not started
- `DOING`: actively in progress
- `DONE`: implemented and checked
- `BLOCKED`: cannot continue without prerequisite/decision

## 4) Phase 1 Execution Board (Closed)

| ID | Task | Deliverable | Status | Notes |
|---|---|---|---|---|
| P1-01 | Create k8s base structure | `k8s/` directories | DONE | `namespace/registry/kafka/ingestor/flink` |
| P1-02 | k3d bootstrap script | `k8s/registry/registry-setup.sh` | DONE | cluster + local registry + NodePort mappings |
| P1-03 | Namespace manifest | `k8s/namespace.yaml` | DONE | namespace: `taxi-pipeline` |
| P1-04 | Kafka headless service | `k8s/kafka/kafka-headless-service.yaml` | DONE | stable DNS for StatefulSet |
| P1-05 | Kafka shared config | `k8s/kafka/kafka-configmap.yaml` | DONE | KRaft quorum + tuning |
| P1-06 | Kafka StatefulSet (3 brokers) | `k8s/kafka/kafka-statefulset.yaml` | DONE | init-based per-pod env + PVC |
| P1-07 | Kafka UI deployment/service | `k8s/kafka/kafka-ui.yaml` | DONE | NodePort `30090` |
| P1-08 | Ingestor config | `k8s/ingestor/ingestor-configmap.yaml` | DONE | `SPRING_*`, `APP_*` tuning |
| P1-09 | Ingestor deployment | `k8s/ingestor/ingestor-deployment.yaml` | DONE | 3 replicas + wait-for-kafka init |
| P1-10 | Ingestor service | `k8s/ingestor/ingestor-service.yaml` | DONE | NodePort `30080` |
| P1-11 | Flink config | `k8s/flink/flink-configmap.yaml` | DONE | `FLINK_*` + `flink-conf.yaml` |
| P1-12 | Flink JobManager deployment | `k8s/flink/flink-jobmanager-deployment.yaml` | DONE | standalone-job args |
| P1-13 | Flink JobManager service | `k8s/flink/flink-jobmanager-service.yaml` | DONE | ClusterIP + NodePort `30081` |
| P1-14 | Flink TaskManager deployment | `k8s/flink/flink-taskmanager-deployment.yaml` | DONE | start with 1 replica |
| P1-15 | External Nginx routing update | `infra/nginx/docker-compose.k8s.yml` + `nginx.k8s.conf.template` | DONE | single upstream via NodePort 30080 |
| P1-16 | Verification runbook | section update in this file | DONE | deploy order + checks |

Phase 1 exit was declared on 2026-02-09. ClickHouse data verification is intentionally excluded from the Phase 1 exit gate.

## 4.5) Phase 1.5 Execution Board

| ID | Task | Deliverable | Status | Notes |
|---|---|---|---|---|
| P15-01 | Create multi-node scaffold | `k8s/multi-node/` directory | DONE | runtime assets scaffolded |
| P15-02 | Build base image | `k8s/multi-node/Dockerfile` | DONE | `rancher/k3s:v1.34.3-k3s1` + Tailscale binaries |
| P15-03 | Runtime entrypoint | `k8s/multi-node/entrypoint.sh` | DONE | role-based startup: tailscaled auth + k3s server/agent launch |
| P15-04 | Team runbook | `k8s/multi-node/README.md` | DONE | server/agent startup + kubeconfig + validation guide |
| P15-05 | Server path validation | run log + command set | TODO | server container joins Tailscale and exposes k3s API |
| P15-06 | Agent join validation | run log + command set | TODO | worker containers join same cluster successfully |
| P15-07 | Registry strategy update | Phase 1.5 notes in runbook | DONE | runbook includes Tailscale-reachable registry approach |
| P15-08 | Manifest reuse check | validation notes | TODO | verify existing Phase 1 manifests deploy unchanged on Phase 1.5 cluster |

## 5) Immediate Next Actions

1. Execute `P15-05` and `P15-06` with one server and at least one agent container.
2. Validate registry pull/push path on Tailscale network and record results.
3. Run `P15-08` by applying existing Phase 1 manifests on the Phase 1.5 cluster.
4. Track all manual checks in `k8s/vertification.md`.

## 6) Decisions Log

- 2026-02-08: Use `orchestration.md` as upstream and this file as downstream implementation tracker.
- 2026-02-08: Implement in vertical slices (Kafka -> Ingestor -> Flink -> Nginx) to reduce blast radius.
- 2026-02-08: Use `FLINK_CLICKHOUSE_HOST=replace-with-aws-clickhouse-host` as safe placeholder; set real endpoint before enabling ClickHouse sink.
- 2026-02-08: Pin local k8s test versions (`kubectl v1.35.0`, `k3d v5.8.3`, cluster image `rancher/k3s:v1.34.3-k3s1`).
- 2026-02-08: Default Flink ClickHouse sink to `false` for initial end-to-end smoke tests.
- 2026-02-09: Declare Phase 1 complete without ClickHouse exit-gate validation; move active implementation scope to Phase 1.5.
- 2026-02-09: For Phase 1.5 runtime, use `rancher/k3s:v1.34.3-k3s1` with copied `tailscaled/tailscale` binaries and env-driven role split (`server|agent`).

## 7) Change Log

- 2026-02-08: Initial downstream implementation tracker created.
- 2026-02-08: Completed `P1-01` to `P1-03` (k8s scaffold + registry script + namespace manifest).
- 2026-02-08: Completed `P1-04` to `P1-07` (Kafka headless service, configmap, statefulset, kafka-ui).
- 2026-02-08: Completed `P1-08` to `P1-14` (Ingestor + Flink manifests).
- 2026-02-08: Completed `P1-16` (verification runbook section added).
- 2026-02-08: Completed `P1-15` (Nginx distributed upstream switched to single NodePort endpoint).
- 2026-02-08: Updated runbooks for post-install execution flow and ClickHouse-deferred smoke testing.
- 2026-02-08: Set default `FLINK_ENABLE_CLICKHOUSE_SINK=false` for first-pass k3d testing.
- 2026-02-09: Closed Phase 1 execution board and opened Phase 1.5 execution board (`P15-01` to `P15-08`).
- 2026-02-09: Completed `P15-01`, `P15-02`, `P15-03`, `P15-04`, and `P15-07` (created `k8s/multi-node` assets and runbook).
- 2026-02-09: Added manual validation runbook `k8s/vertification.md` for operator-driven checks (`P15-05`, `P15-06`, `P15-08`).

## 8) Verification Runbook

### 8.0 Version Pin (for reproducibility)

- `kubectl`: `v1.35.0`
- `k3d`: `v5.8.3`
- `k3s image for cluster`: `rancher/k3s:v1.34.3-k3s1`
- `k3s host binary (optional)`: `v1.34.3+k3s1`

### 8.1 Preflight

```bash
command -v docker
command -v k3d
command -v kubectl
kubectl version --client
k3d version
```

### 8.2 Create k3d Registry + Cluster

```bash
# Optional explicit override (same as default in script):
# K3S_IMAGE=rancher/k3s:v1.34.3-k3s1 bash k8s/registry/registry-setup.sh
bash k8s/registry/registry-setup.sh
```

### 8.3 Build and Push Images

```bash
# Ingestor image
docker build -t localhost:5000/taxi-ingestor:latest -f services/ingestor/Dockerfile services/ingestor/
docker push localhost:5000/taxi-ingestor:latest

# Flink image (build jar first)
cd services/flink-job
mvn clean package -DskipTests
docker build -t localhost:5000/taxi-flink-job:latest -f Dockerfile .
docker push localhost:5000/taxi-flink-job:latest
cd ../..
```

Note: k8s manifests use `k3d-taxi-registry:5000/...`; pushing to `localhost:5000/...` targets the same local k3d registry.

### 8.4 Deploy Order

```bash
kubectl apply -f k8s/namespace.yaml

kubectl apply -f k8s/kafka/
kubectl wait --for=condition=ready pod -l app=kafka -n taxi-pipeline --timeout=180s

kubectl apply -f k8s/ingestor/
kubectl wait --for=condition=ready pod -l app=ingestor -n taxi-pipeline --timeout=180s

kubectl apply -f k8s/flink/
kubectl wait --for=condition=ready pod -l app=flink,component=jobmanager -n taxi-pipeline --timeout=180s
```

### 8.5 Checks

```bash
kubectl get pods -n taxi-pipeline

kubectl logs kafka-0 -n taxi-pipeline --tail=100
kubectl logs -l app=ingestor -n taxi-pipeline --tail=100
kubectl logs -l app=flink,component=jobmanager -n taxi-pipeline --tail=100

# Kafka UI
echo "Open: http://localhost:30090"

# Flink UI (NodePort from k3d mapping)
echo "Open: http://localhost:30081"
```

### 8.6 External Nginx (k8s ingress path)

Before running external Nginx, set the upstream endpoint to the k8s NodePort in `config/.env`:

- `INGESTOR_1_IP=<k3d-node-ip-or-127.0.0.1>`
- `INGESTOR_1_PORT=30080`

Then run Nginx using the **k8s-specific** compose file (not the distributed one):

```bash
docker compose -f infra/nginx/docker-compose.k8s.yml --env-file config/.env up -d
```

Note: `docker-compose.k8s.yml` uses `nginx.k8s.conf.template` which has a single upstream.
K8s handles internal load balancing via kube-proxy; Nginx only needs the NodePort endpoint.
Do NOT use `docker-compose.distributed.yml` here — that file has 3 upstreams for non-k8s distributed mode.
