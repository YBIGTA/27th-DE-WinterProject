---
status: DONE
created: 2026-02-04
purpose: track and resolve all issues found after demolish-ops refactor
---

# Merge Issue Tracker

## Fix 1 — Port conflict 8081 (CRITICAL)
- **Status:** DONE
- **Problem:** `INGESTOR_1_PORT` and `FLINK_JOBMANAGER_PORT` both = 8081 in single-machine `.env`
- **Fix:** Changed `FLINK_JOBMANAGER_PORT` to `8084` in `config/.env` and `config/.env.single-machine`

## Fix 2 — ClickHouse missing network (CRITICAL)
- **Status:** DONE
- **Problem:** `infra/clickhouse/docker-compose.yml` (and distributed) had no `networks:` — Flink couldn't resolve `clickhouse` hostname
- **Fix:** Attached `clickhouse` service to `kafka-network` (external) in both `docker-compose.yml` and `docker-compose.distributed.yml`

## Fix 3 — Nginx missing network (CRITICAL)
- **Status:** DONE
- **Problem:** `infra/nginx/docker-compose.yml` had no `networks:` — couldn't resolve `ingestor-*` hostnames
- **Fix:** Attached `nginx-lb` to `kafka-network` (external) in single-machine compose

## Fix 4 — Distributed Kafka: external network never created (HIGH)
- **Status:** DONE
- **Problem:** `infra/kafka/docker-compose.distributed.yml` declared `kafka-network` as `external: true` but nothing created it — compose up would fail immediately
- **Fix:** Removed `external: true`, changed to a regular bridge network so compose creates it locally

## Fix 5 — ClickHouse healthcheck uses host port (HIGH)
- **Status:** DONE
- **Problem:** Healthcheck inside container pinged `${CLICKHOUSE_HTTP_PORT}` (host-side port) instead of the container-internal port `8123`
- **Fix:** Hardcoded `8123` in healthcheck test in both `docker-compose.yml` and `docker-compose.distributed.yml`

## Fix 6 — Flink: CSV file unreachable at runtime (HIGH)
- **Status:** DONE
- **Problem:** `SpatialJoinFunction.java` read `/opt/flink/resources/taxi_zone_median_coords.csv` via `FileReader` — file is bundled in JAR, not on filesystem; no volume mount existed
- **Fix:** Switched to `getClass().getClassLoader().getResourceAsStream("taxi_zone_median_coords.csv")` with `InputStreamReader`; removed unused `FileReader` import

## Fix 7 — Distributed nginx: missing `least_conn` (MEDIUM)
- **Status:** DONE
- **Problem:** Single-machine nginx used `least_conn;` but distributed template defaulted to round-robin
- **Fix:** Added `least_conn;` to upstream block in `infra/nginx/templates/nginx.distributed.conf.template`

## Fix 8 — Single-machine nginx: missing `env_file` (MEDIUM)
- **Status:** DONE
- **Problem:** All other composes had `env_file: ../../config/.env`; single-machine nginx did not
- **Fix:** Added `env_file: ../../config/.env` to `infra/nginx/docker-compose.yml`

## Fix 9 — Distributed: all brokers/ingestors shared same ports (HIGH)
- **Status:** DONE
- **Problem:** All 3 Kafka brokers used the same `KAFKA_EXTERNAL_PORT`, `KAFKA_INTERNAL_PORT`, `KAFKA_CONTROLLER_PORT`. All 3 ingestors used `INGESTOR_*_PORT=8080`. Running everything locally on one machine caused port collisions on every layer.
- **Fix:** Switched to per-broker port variables across the distributed stack:
  - `infra/kafka/docker-compose.distributed.yml` — per-broker ports in mappings, `ADVERTISED_LISTENERS`, `CONTROLLER_QUORUM_VOTERS`, kafka-ui bootstrap
  - `infra/flink/docker-compose.distributed.yml` — bootstrap servers use per-broker external ports
  - `services/ingestor/docker-compose.distributed.yml` — bootstrap servers + ingestor host ports (8081/8082/8083)
  - `config/.env.distributed` — added `KAFKA_N_EXTERNAL_PORT`, `KAFKA_N_INTERNAL_PORT`, `KAFKA_N_CONTROLLER_PORT`; ingestor ports set to 8081/8082/8083; `FLINK_JOBMANAGER_PORT` set to 8084
- **Port map (local run):**

| Service | Host ports |
|---|---|
| kafka-1 | 9092 (ext), 29092 (int), 19093 (ctrl) |
| kafka-2 | 9094 (ext), 29094 (int), 19094 (ctrl) |
| kafka-3 | 9096 (ext), 29096 (int), 19095 (ctrl) |
| ingestor-1 | 8081 |
| ingestor-2 | 8082 |
| ingestor-3 | 8083 |
| flink-jm | 8084 |
| nginx | 8080 |
| clickhouse | 8123 / 9000 |
| kafka-ui | 8090 |
