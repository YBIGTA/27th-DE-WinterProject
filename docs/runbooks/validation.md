# Pipeline Validation Guide (No Kubernetes)

Use this after starting the pipeline to verify it is actually working end-to-end.

## Scope
- Single-machine mode
- Distributed multi-machine mode
- Core path validation:
  - `generator -> nginx -> ingestor -> kafka -> flink -> clickhouse`
  - `flink -> clickhouse.taxi_predictions` (ONNX 예측 sink)
- Monitoring path validation:
  - `kafka -> kafka-exporter -> prometheus -> grafana`
  - `clickhouse -> grafana`

## 0) Load env
Run from project root:

```bash
set -a
source config/.env
set +a
```

## 1) Base service checks
These checks should pass before running generator.

```bash
# nginx
curl -sf "http://${NGINX_IP}:${NGINX_LB_PORT}/health"

# ingestor
curl -sf "http://${INGESTOR_1_IP}:${INGESTOR_1_PORT}/health"
curl -sf "http://${INGESTOR_2_IP}:${INGESTOR_2_PORT}/health"
curl -sf "http://${INGESTOR_3_IP}:${INGESTOR_3_PORT}/health"

# clickhouse
curl -sf "http://${CLICKHOUSE_IP}:${CLICKHOUSE_HTTP_PORT}/ping"

# flink web ui
curl -sf "http://${FLINK_IP}:${FLINK_JOBMANAGER_PORT}" >/dev/null
```

Expected:
1. `/health` returns HTTP 200.
2. ClickHouse `/ping` returns `Ok`.
3. Flink UI endpoint responds without error.

## 2) Kafka checks
Run on a machine where `kafka-1` container exists.

```bash
# list topics
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# topic detail
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
```

Expected:
1. `taxi-event-data` exists.
2. Topic describe returns partition metadata without errors.

## 3) Ingest path checks (before generator)
This sends one test event through `nginx -> ingestor -> kafka`.

```bash
NOW_UTC="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"

curl -s -o /tmp/ingest_check.out -w "%{http_code}\n" \
  -X POST "http://${NGINX_IP}:${NGINX_LB_PORT}/ingest" \
  -H "Content-Type: application/json" \
  -d "{\"event\":\"PICKUP\",\"trip_id\":999000001,\"ts\":\"${NOW_UTC}\",\"lat\":40.7580,\"lon\":-73.9855}" > /tmp/ingest_check.code

cat /tmp/ingest_check.code
```

Expected:
1. HTTP status is `202` (or `429` only when load is saturated).

## 4) End-to-end data checks (with generator)
1. Start generator.
2. In another terminal, run:

```bash
# row count snapshot 1
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"

sleep 10

# row count snapshot 2
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"

# latest rows
docker exec clickhouse clickhouse-client -q "SELECT trip_id, ts, zone_id, event FROM default.taxi_events ORDER BY ts DESC LIMIT 10"

# prediction count / latest predictions
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_predictions"
docker exec clickhouse clickhouse-client -q "SELECT prediction_time, target_time, zone_id, predicted_demand, model_version FROM default.taxi_predictions ORDER BY prediction_time DESC LIMIT 10"
```

Expected:
1. Snapshot 2 count is greater than snapshot 1.
2. Recent rows are populated while generator runs.
3. `taxi_predictions`는 초기 warm-up 구간 이후 증가한다.

참고:
- 기본 `FLINK_MODEL_FEATURE_LAG_STEPS=20`, `FLINK_MODEL_INTERVAL_MINUTES=3`이므로 zone별 첫 예측까지 약 60분의 히스토리가 필요할 수 있습니다.

## 5) Flink runtime checks
Run where Flink containers are running.

```bash
# taskmanager logs (single-machine example)
docker logs --tail 100 flink-taskmanager-1

# jobmanager logs
docker logs --tail 100 flink-jobmanager
```

Expected:
1. No continuous connector/bootstrap failure loops.
2. No persistent ClickHouse JDBC sink failures.

## 6) Grafana checks
Verify Grafana is running and datasources are connected.

```bash
# grafana health
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"

# list datasources (should show clickhouse and prometheus)
curl -sf -u admin:admin "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/datasources" | python3 -m json.tool

# prometheus health
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"

# prometheus has kafka-exporter target
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/targets" | python3 -c "
import sys, json
targets = json.load(sys.stdin)['data']['activeTargets']
for t in targets:
    print(f\"{t['labels'].get('job','?'):20s} {t['health']:6s} {t['lastScrape']}\")
"
```

Expected:
1. `/api/health` returns `{"database":"ok", ...}`.
2. Datasource list includes `ClickHouse` and `Prometheus`.
3. Prometheus `/-/healthy` returns `Prometheus Server is Healthy.`
4. Prometheus targets show `kafka-exporter` with health `up`.

Open Grafana in browser at `http://localhost:3000` (login `admin` / `admin`).
Navigate to Dashboards > NYC Taxi Events. Panels should populate once data flows through the pipeline.

## 7) Distributed-specific notes
1. Run `docker exec kafka-1 ...` only on host that actually runs `kafka-1`.
2. Run `docker exec clickhouse ...` only on host that actually runs `clickhouse`.
3. If a host uses compose files with `external: true` `kafka-network`, ensure:

```bash
docker network ls | rg kafka-network
```

4. If Flink cannot consume Kafka in distributed mode, first verify `KAFKA_*_IP` values in `config/.env` are real reachable host IPs (not loopback).

## 8) Quick failure triage
1. `curl /health` fails:
   - check `docker compose ... ps`
   - check `docker compose ... logs -f`
2. Topic missing:
   - verify Kafka cluster is healthy and brokers are reachable.
3. ClickHouse count not increasing:
   - inspect ingestor logs, then Flink logs.
4. Frequent `429` on ingest:
   - reduce generator speed or tune ingestor buffering settings.
5. Grafana panels show "No data":
   - verify ClickHouse datasource is connected: Grafana > Settings > Data Sources > ClickHouse > Test.
   - verify `default.taxi_events` table has rows: `docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"`.
6. Prometheus targets down:
   - check kafka-exporter container: `docker logs kafka-exporter`.
   - verify Kafka brokers are reachable from the `kafka-network`.
