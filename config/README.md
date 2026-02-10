# Full Pipeline Runbook (No Kubernetes)

This is the canonical runbook for this branch.

Validation checklist: `config/VALIDATION.md`

## Scope
- Deployment modes:
  - Single-machine (no Kubernetes)
  - Distributed multi-machine (no Kubernetes)
- Core pipeline:
  - `generator -> nginx -> ingestor -> kafka -> flink -> clickhouse`
- Monitoring:
  - `kafka -> kafka-exporter -> prometheus -> grafana`
  - `clickhouse -> grafana`
- Optional branches:
  - `kafka -> kafka connect s3 sink -> S3`

## Core rules
1. `config/.env` contains network values only (`*_IP`, `*_PORT`).
2. Non-network runtime values are hardcoded in component compose files.
3. Start components by their own compose files (no root launcher compose).
4. Always pass `--env-file config/.env` to `docker compose`.

## Source of truth
| Layer | File(s) | Contains |
|---|---|---|
| Network | `config/.env.single-machine`, `config/.env.distributed`, `config/.env` | IP/PORT only |
| Kafka runtime | `infra/kafka/docker-compose*.yml` | Broker topology and Kafka tuning |
| ClickHouse runtime | `infra/clickhouse/docker-compose*.yml` | Service wiring and schema bootstrap |
| Ingestor runtime | `services/ingestor/docker-compose*.yml` | `SPRING_*`, `APP_*` |
| Nginx runtime | `infra/nginx/docker-compose*.yml` | Load balancing config |
| Flink runtime | `infra/flink/docker-compose*.yml` | `FLINK_*` runtime values |
| Prometheus runtime | `infra/prometheus/docker-compose*.yml` | Scrape targets, retention |
| Grafana runtime | `infra/grafana/docker-compose*.yml` | Datasources, dashboards |
| Generator runtime | `services/generator/config/default.yaml` | Native generator defaults |

## 0) Prerequisites
1. Docker Engine + Docker Compose plugin.
2. JDK 17+ and Maven for `services/flink-job` build.
3. `uv`, Conan, CMake toolchain for `services/generator` build.
4. Optional data preprocess: Python via `uv` in `data/`.

## 1) Build artifacts (once or when code changes)
Build Flink job JAR:

```bash
cd services/flink-job
mvn clean package
cd ../..
```

Build generator binary:

```bash
cd services/generator
uv --project ../../data run conan profile detect --force
uv --project ../../data run conan install . -of build --build=missing
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
cd ../..
```

Optional data preprocess:

```bash
cd data
uv sync
uv run python preprocess/preprocess_taxi_data.py
cd ..
```

## 2) Single-machine full run (no Kubernetes)
Activate env:

```bash
cp config/.env.single-machine config/.env
```

Start pipeline:

All compose commands below are foreground mode. Run each in a separate terminal and keep them alive.

```bash
# 1) Kafka (also creates docker network kafka-network)
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up

# 2) ClickHouse
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up

# 3) Ingestor cluster (3 instances)
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up --build

# 4) Nginx load balancer
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up

# 5) Flink (jobmanager + 3 taskmanagers; builds image from services/flink-job)
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up --build

# 6) Prometheus + Kafka Exporter (monitoring metrics)
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env up

# 7) Grafana (dashboards — depends on ClickHouse and Prometheus)
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env up
```

Run generator:

```bash
cd services/generator
./build/generate
```

Access Grafana at `http://localhost:3000` (login `admin` / `admin`).
The pre-provisioned "NYC Taxi Events" dashboard shows real-time event metrics once data flows through the pipeline.

## 3) Distributed full run (multi-machine, no Kubernetes)
### 3.1 Shared setup on every machine
1. Place the same repository checkout on each machine.
2. Copy and edit env file:

```bash
cp config/.env.distributed config/.env
```

3. Fill `config/.env` with real machine IPs and ports.
4. For machines that run compose files with `external: true` `kafka-network`, create it once:

```bash
docker network create kafka-network || true
```

Note: `infra/kafka/docker-compose.distributed.yml` creates `kafka-network` itself, but `infra/clickhouse/docker-compose.distributed.yml` and `infra/flink/docker-compose.distributed.yml` expect it to exist.

### 3.2 Startup order
Kafka brokers:

```bash
# kafka machine 1
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1

# kafka machine 2
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-2

# kafka machine 3 (+ kafka ui)
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-3 kafka-ui
```

ClickHouse:

```bash
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse
```

Ingestor cluster:

```bash
# Option A: one host runs all 3 ingestors
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-1 ingestor-2 ingestor-3

# Option B: one ingestor per host
# host-a
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-1
# host-b
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-2
# host-c
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-3
```

Nginx load balancer:

```bash
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up nginx-lb
```

Flink:

```bash
# jobmanager host
cd services/flink-job && mvn clean package && cd ../..
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-jobmanager

# each taskmanager host (build image locally once, then run assigned service)
cd services/flink-job && mvn clean package && cd ../..
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env build flink-jobmanager

# tm host 1
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up flink-taskmanager-1
# tm host 2
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up flink-taskmanager-2
# tm host 3
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up flink-taskmanager-3
```

Run generator on generator host:

```bash
cd services/generator
INGEST_URL="http://${NGINX_IP}:${NGINX_LB_PORT}/ingest" ./build/generate
```

## 4) Runtime validation (both modes)
Health and UI:

```bash
# load values from config/.env into current shell
set -a
source config/.env
set +a

# kafka-ui
curl -s "http://${KAFKA_3_IP}:${KAFKA_UI_PORT}" >/dev/null

# clickhouse
curl -s "http://${CLICKHOUSE_IP}:${CLICKHOUSE_HTTP_PORT}/ping"

# flink web ui
curl -s "http://${FLINK_IP}:${FLINK_JOBMANAGER_PORT}" >/dev/null

# ingestor direct health
curl -s "http://${INGESTOR_1_IP}:${INGESTOR_1_PORT}/health"

# prometheus
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"

# grafana
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
```

Check topic and event flow:

```bash
# topic list (run on kafka-1 host)
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# clickhouse row count (run on clickhouse host)
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"
```

Expected behavior:
1. Kafka topic `taxi-event-data` exists.
2. ClickHouse row count increases while generator is running.
3. Prometheus `/-/healthy` returns `Prometheus Server is Healthy.`
4. Grafana `/api/health` returns JSON with `"database": "ok"`.

## 5) Optional: S3 sink branch
1. Provision S3 + IAM in `infra/terraform`:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

2. Prepare connector config:

```bash
cd ../connectors
cp s3-sink-config.template.json s3-sink-config.json
```

3. Fill bucket and AWS keys in `infra/connectors/s3-sink-config.json`.
4. Start Kafka Connect worker (not included in this repository).
5. Register connector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @s3-sink-config.json \
  http://localhost:8083/connectors
```

## 6) Stop and cleanup
Single-machine stop (reverse order):

```bash
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env down
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env down
docker compose -f infra/flink/docker-compose.yml --env-file config/.env down
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env down
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env down
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env down
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down
```

Distributed stop: run the matching `down` command on each machine with its distributed compose file.

## 7) Common pitfalls
1. Missing `--env-file config/.env` causes unresolved `${VAR}` or wrong defaults.
2. `config/.env` with non-network keys breaks source-of-truth policy.
3. `kafka-network` missing on non-kafka machines breaks ClickHouse/Flink startup.
4. Flink TaskManager machine without local `flink-taxi-job:latest` image fails to start.
5. Single-machine Flink compose uses internal Docker DNS (`kafka-*`, `clickhouse`); only distributed mode should use real reachable host IPs in `config/.env`.

## Do not commit
Do not commit real-value `config/.env`.
