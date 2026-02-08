# 4-Way Execution Reference

All 4 deployment modes coexist independently. Each mode has its own config, compose, and nginx files. No mode modifies another's files.

## Mode-to-File Mapping

| Mode | Env Template | Nginx Compose | Nginx Config | Infra Compose | K8s Manifests |
|------|-------------|---------------|-------------|---------------|---------------|
| 1. Single w/o K8s | `.env.single-machine` | `docker-compose.yml` | `nginx.single-machine.conf` | `docker-compose.yml` | N/A |
| 2. Distributed w/o K8s | `.env.distributed` | `docker-compose.distributed.yml` | `nginx.distributed.conf.template` | `docker-compose.distributed.yml` | N/A |
| 3. Single w/ K8s | `.env.single-machine` + `INGESTOR_1_PORT=30080` | `docker-compose.k8s.yml` | `nginx.k8s.conf.template` | N/A | `k8s/*.yaml` |
| 4. Distributed w/ K8s | `.env.distributed` + NodePort override | `docker-compose.k8s.yml` | `nginx.k8s.conf.template` | N/A | `k8s/*.yaml` + Tailscale |

## Port Assignments

| Service | Mode 1 (SM) | Mode 2 (Distributed) | Mode 3 (K8s SM) | Mode 4 (K8s Dist) |
|---------|------------|---------------------|----------------|-------------------|
| Nginx LB | 8080 | 8080 | 8080 | 8080 |
| Ingestor 1/2/3 | 8081/8082/8083 | 8081/8082/8083 | NodePort 30080 | NodePort 30080 |
| Kafka (external) | 9092/9094/9096 | 9092/9094/9096 | N/A (internal) | N/A (internal) |
| Kafka UI | 8090 | 8090 | NodePort 30090 | NodePort 30090 |
| Flink UI | 8084 | 8084 | NodePort 30081 | NodePort 30081 |
| ClickHouse HTTP | 8123 | 8123 | (AWS endpoint) | (AWS endpoint) |

---

## Mode 1: Single Machine w/o Kubernetes

```bash
# 0. Activate config
cp config/.env.single-machine config/.env

# 1. Kafka
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up

# 2. ClickHouse
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up

# 3. Ingestor + Nginx
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up

# 4. Flink
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up

# 5. Generator (native)
cd services/generator && ./build/generate
```

**Nginx:** Static config `nginx.single-machine.conf` with 3 Docker upstreams (`ingestor-1:8080`, `ingestor-2:8080`, `ingestor-3:8080`). All containers share `kafka-network` Docker bridge.

---

## Mode 2: Distributed w/o Kubernetes

```bash
# 0. Activate config
cp config/.env.distributed config/.env
# Edit config/.env with actual machine IPs

# Kafka (each on separate machine/terminal)
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-2
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-3 kafka-ui

# ClickHouse
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse

# Ingestor + Nginx
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up nginx-lb

# Flink
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up flink-jobmanager flink

# Generator (native)
cd services/generator && ./build/generate
```

**Nginx:** Template `nginx.distributed.conf.template` with 3 upstreams (`${INGESTOR_1_IP}:${INGESTOR_1_PORT}`, `_2_`, `_3_`). Variables resolved via `envsubst` from `.env`.

---

## Mode 3: Single Machine w/ Kubernetes (k3d)

```bash
# 0. Create k3d cluster + local registry
bash k8s/registry/registry-setup.sh

# 1. Build and push images
docker build -t localhost:5000/taxi-ingestor:latest -f services/ingestor/Dockerfile services/ingestor/
docker push localhost:5000/taxi-ingestor:latest
cd services/flink-job && mvn clean package -DskipTests
docker build -t localhost:5000/taxi-flink-job:latest -f Dockerfile .
docker push localhost:5000/taxi-flink-job:latest
cd ../..

# 2. Deploy K8s manifests (order matters)
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl wait --for=condition=ready pod -l app=kafka -n taxi-pipeline --timeout=180s
kubectl apply -f k8s/ingestor/
kubectl apply -f k8s/flink/

# 3. External Nginx (points to K8s NodePort)
cp config/.env.single-machine config/.env
# Edit config/.env: set INGESTOR_1_PORT=30080
docker compose -f infra/nginx/docker-compose.k8s.yml --env-file config/.env up

# 4. Generator (native)
cd services/generator && ./build/generate
```

**Nginx:** Template `nginx.k8s.conf.template` with 1 upstream (`${INGESTOR_1_IP}:${INGESTOR_1_PORT}`). K8s handles internal load balancing via kube-proxy. Nginx only needs the single NodePort entry.

**UIs:**
- Kafka UI: http://localhost:30090
- Flink UI: http://localhost:30081

---

## Mode 4: Distributed w/ Kubernetes (k3s + Tailscale)

> Status: Not yet implemented. Planned for Phase 1.5.

Will use custom Docker image containing k3s + Tailscale. Each machine runs the image, containers join as one k3s cluster across machines via Tailscale mesh.

```bash
# 0. Activate config
cp config/.env.distributed config/.env
# Edit config/.env with Tailscale IPs and INGESTOR_1_PORT=30080

# 1. K3s cluster setup (TBD)

# 2. Deploy K8s manifests (same as Mode 3)
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/ingestor/
kubectl apply -f k8s/flink/

# 3. External Nginx (same compose as Mode 3)
docker compose -f infra/nginx/docker-compose.k8s.yml --env-file config/.env up

# 4. Generator (native)
cd services/generator && ./build/generate
```

**Nginx:** Same as Mode 3 — `docker-compose.k8s.yml` + `nginx.k8s.conf.template`. The `.env` file provides the Tailscale IP of the K8s node.

---

## Key Rules

1. **Never modify another mode's files.** Each mode has its own nginx config/compose. K8s modes use `*.k8s.*` files; distributed w/o K8s uses `*.distributed.*` files.
2. **`config/.env` is the active config.** Copy the right template before launching.
3. **K8s manifests are self-contained** under `k8s/`. Docker-compose files are untouched by K8s.
4. **External Nginx** is always needed for the Generator → Ingestor path (Generator runs natively outside any cluster/compose).
