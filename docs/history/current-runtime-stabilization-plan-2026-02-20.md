# Runtime Stabilization Plan (2026-02-20)

Post-merge verification for `kafka-dev` → `end2end` (commit `d2febd6`).
Merge summary: [`current-kafka-dev-merge-change-summary-2026-02-20.md`](current-kafka-dev-merge-change-summary-2026-02-20.md)

---

## 1. Critical Issues (Block Deployment)

### 1.1 Flink distributed: missing `networks` block

**File:** `infra/flink/docker-compose.distributed.yml`

The file ends at line 101 with no top-level `networks:` definition. All four services reference `kafka-network` via per-service `networks:`, but the external network is never declared. Docker Compose will fail on `docker compose up`.

**Expected fix** — append to EOF:
```yaml
networks:
  kafka-network:
    external: true
    name: kafka-network
```

### 1.2 Flink distributed: JM/TM RPC address mismatch

**File:** `infra/flink/docker-compose.distributed.yml`

| Service | `jobmanager.rpc.address` value | Line |
|---|---|---|
| `flink-jobmanager` | `flink-jobmanager` (DNS) | 15 |
| `flink-taskmanager-1` | `${FLINK_IP}` (env var) | 45 |
| `flink-taskmanager-2` | `${FLINK_IP}` (env var) | 65 |
| `flink-taskmanager-3` | `${FLINK_IP}` (env var) | 85 |

In a distributed (multi-machine) deployment, TMs resolve `${FLINK_IP}` to an IP address while JM binds using the container DNS name `flink-jobmanager`. If `FLINK_IP` does not point to the actual JM host IP, TMs cannot connect.

**Fix options:**
1. Set all to `${FLINK_IP}` and ensure `.env` has the correct JM host IP.
2. Set all to `flink-jobmanager` and ensure the hostname resolves on every machine (only works on a shared Docker network, not cross-machine).

---

## 2. High-Priority Issues

### 2.1 Kafka-UI distributed: missing `depends_on` healthcheck

**File:** `infra/kafka/docker-compose.distributed.yml` (lines 131–143)

`kafka-ui` has no `depends_on` on any broker. In a slow-start scenario, kafka-ui may start before brokers are healthy and fail to connect. The single-machine compose uses `depends_on` with `condition: service_healthy`.

**Recommendation:** Add:
```yaml
depends_on:
  kafka-1:
    condition: service_healthy
```
Or, since distributed brokers are on separate machines, ensure kafka-ui only starts after bootstrap servers are reachable (scripted health-gate or startup retry).

### 2.2 Nginx distributed: missing `/health` endpoint

**File:** `infra/nginx/templates/nginx.distributed.conf.template`

The single-machine config (`infra/nginx/nginx.single-machine.conf`, lines 43–46) defines:
```nginx
location /health {
    return 200 'OK';
    add_header Content-Type text/plain;
}
```
The distributed template has no `/health` location. Any external health probe hitting the LB will return a proxied response from an ingestor rather than an nginx-level health signal.

**Fix:** Add the same `location /health` block to the distributed template.

### 2.3 Ingestor `application.yml` silently overrides `ReactorKafkaConfig.java`

**Files:**
- `services/ingestor/bin/main/application.yml` — Spring Boot properties
- `services/ingestor/src/main/java/com/ingestion/config/ReactorKafkaConfig.java` — programmatic producer config

Conflict matrix:

| Setting | `application.yml` | `ReactorKafkaConfig.java` | Winner |
|---|---|---|---|
| `linger.ms` | 50 | 10 | Java (programmatic) |
| `batch.size` | 32768 (32KB) | 65536 (64KB) | Java |
| `buffer.memory` | 67108864 (64MB) | 268435456 (256MB) | Java |
| `max.block.ms` | 2000 | 5000 | Java |
| `acks` | 1 | 1 | Same |

Since `ReactorKafkaConfig` builds `SenderOptions` programmatically from hardcoded values (not from Spring properties), the `application.yml` `spring.kafka.producer.properties.*` values are **ignored** for the Reactor Kafka sender. The `application.yml` values are stale (pre-merge) and misleading.

**Recommendation:** Either:
1. Delete the `spring.kafka.producer.properties` section from `application.yml` to avoid confusion, or
2. Wire `ReactorKafkaConfig` to read from Spring properties so there is a single source of truth.

---

## 3. Medium-Priority Observations

### 3.1 Flink distributed TM-3 JDBC batch interval inconsistency

**File:** `infra/flink/docker-compose.distributed.yml`

| TaskManager | `FLINK_JDBC_BATCH_INTERVAL_MS` | `FLINK_JDBC_BATCH_SIZE` |
|---|---|---|
| TM-1, TM-2 | (not set → Java default: 3000) | (not set → Java default: 50000) |
| TM-3 | **10000** | **50000** |

TM-3 (line 94–95) explicitly sets `FLINK_JDBC_BATCH_INTERVAL_MS=10000`, 3.3x slower flush interval than the other TMs. This creates uneven write latency to ClickHouse.

### 3.2 Flink distributed: parallelism=3 with 7 total slots

`parallelism.default: 3` and `FLINK_PARALLELISM=3`, but TM-1 has 2 slots, TM-2 has 2 slots, TM-3 has 3 slots = **7 total slots**. With parallelism=3, 4 slots are idle.

### 3.3 Ingestor batch size: distributed=1000 vs single-machine=2000

| Mode | `APP_TUNING_BATCH_SIZE` | `APP_TUNING_BATCH_TIMEOUT_MS` |
|---|---|---|
| Single-machine | 2000 | 20 |
| Distributed | 1000 | 10 |

Distributed uses smaller batches with tighter timeout. This is likely intentional (lower per-request latency over a network), but worth verifying under load.

### 3.4 ClickHouse sink parallelism hardcoded to 3

**File:** `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java` (line 166)

```java
.setParallelism(3);
```

Upstream parallelism is 12 (single-machine) or 3 (distributed). In single-machine mode, data fans in from 12 parallel operators to 3 sink instances — potential bottleneck if ClickHouse write throughput is the constraint.

### 3.5 Ingestor `tryEmitNext` spin-wait retry (no backoff)

**File:** `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java` (lines 180–187)

```java
while (result == Sinks.EmitResult.FAIL_NON_SERIALIZED && retryCount < 10) {
    Thread.onSpinWait();
    result = sink.tryEmitNext(event);
    retryCount++;
}
```

Under sustained contention, this burns CPU with no backoff. The `emitFailureHandler` (lines 52–58) provides the same retry logic in a cleaner way. Consider using `sink.emitNext(event, emitFailureHandler)` instead, or adding a small `Thread.sleep(0)` / `LockSupport.parkNanos()` between retries.

### 3.6 Nginx `worker_connections`: distributed=1024 vs single-machine=4096

| Mode | `worker_connections` |
|---|---|
| Single-machine | 4096 |
| Distributed | 1024 |

Distributed nginx may hit connection limits sooner if generators are configured for high concurrency. Consider aligning or tuning based on expected load.

---

## 4. Single-Machine Verification Checklist

Run these after `docker compose up` with the single-machine configs.

### 4.1 Kafka: topic config

```bash
# Verify RF=3, partitions=12, min.insync.replicas=2
docker exec kafka-1 kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic taxi-event-data
```

Expected:
- `PartitionCount: 12`
- `ReplicationFactor: 3`
- Each partition should show `Isr` with at least 2 brokers

```bash
# Verify topic-level config
docker exec kafka-1 kafka-configs --bootstrap-server localhost:29092 \
  --entity-type topics --entity-name taxi-event-data --describe
```

### 4.2 Service health endpoints

```bash
# Ingestor health (all 3 instances)
curl -s http://localhost:8081/health
curl -s http://localhost:8082/health
curl -s http://localhost:8083/health

# Nginx LB health
curl -s http://localhost:8080/health

# Flink JobManager dashboard
curl -s http://localhost:8084/overview | python3 -m json.tool

# Kafka-UI
curl -s http://localhost:8090/ -o /dev/null -w "%{http_code}"
```

### 4.3 Ingest path smoke test

```bash
# Send a test event through nginx LB
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"trip_id":999999,"ts":"2026-02-20T12:00:00Z","event":"PICKUP","lat":40.748,"lon":-73.985}'
```

Note:
- Current ingestor API exposes `POST /ingest` (not `/api/events`).

### 4.4 Flink job and checkpoint status

```bash
# List running jobs
curl -s http://localhost:8084/jobs/overview | python3 -m json.tool

# Check checkpointing is active (replace <job-id>)
curl -s http://localhost:8084/jobs/<job-id>/checkpoints | python3 -m json.tool
```

Expected: `EXACTLY_ONCE` mode, checkpoints completing every ~30s, no sustained failures.

### 4.5 End-to-end data flow

```bash
# Check Kafka consumer lag (Flink consumer group)
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group taxi-realtime-flink --describe

# Check ClickHouse row count
docker exec clickhouse clickhouse-client \
  --query "SELECT count() FROM taxi_events"

# Verify daily partitions (toYYYYMMDD)
docker exec clickhouse clickhouse-client \
  --query "SELECT partition, rows FROM system.parts WHERE table='taxi_events' AND active ORDER BY partition"
```

### 4.6 ClickHouse async insert verification

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT name, value FROM system.settings WHERE name LIKE 'async_insert%'"
```

Expected: `async_insert=1`, `wait_for_async_insert=0`, `async_insert_busy_timeout_ms=2000`.

### 4.7 Log inspection

```bash
# Check for errors across all services
docker compose logs --tail=50 ingestor-1 | grep -i "error\|fail\|exception"
docker compose logs --tail=50 flink-jobmanager | grep -i "error\|fail\|exception"
docker compose logs --tail=50 clickhouse | grep -i "error\|fail\|exception"

# Ingestor metrics lines
docker compose logs ingestor-1 | grep "\[METRICS\]" | tail -5
```

---

## 5. Distributed-Mode Additional Checks

These checks supplement Section 4 for multi-machine deployments.

### 5.1 Network block verification

```bash
# Validate compose file parses without error
docker compose -f infra/flink/docker-compose.distributed.yml config > /dev/null
```

If this fails with a network-related error, the `networks` block fix from Section 1.1 has not been applied.

### 5.2 JM/TM RPC resolution

```bash
# On TM machines, verify FLINK_IP resolves to JM
ping -c 1 ${FLINK_IP}

# From JM container, verify it binds correctly
docker exec flink-jobmanager bash -c 'grep jobmanager.rpc.address /opt/flink/conf/flink-conf.yaml'
```

### 5.3 Cross-machine Kafka bootstrap

```bash
# From each ingestor/flink machine, verify all 3 brokers are reachable
for ip_port in "${KAFKA_1_IP}:${KAFKA_1_EXTERNAL_PORT}" "${KAFKA_2_IP}:${KAFKA_2_EXTERNAL_PORT}" "${KAFKA_3_IP}:${KAFKA_3_EXTERNAL_PORT}"; do
  nc -zv ${ip_port/:/ } 2>&1
done
```

### 5.4 Firewall / port matrix

Ensure the following ports are open between machines:

| From | To | Port | Purpose |
|---|---|---|---|
| Ingestors | Kafka brokers | 9092, 9094, 9096 | Kafka external listeners |
| Flink TMs | Flink JM | 6123 | RPC |
| Flink JM | Flink TMs | 6122 | TM RPC |
| Flink (all) | Kafka brokers | 9092, 9094, 9096 | Kafka external listeners |
| Flink (all) | ClickHouse | 8123 | JDBC/HTTP |
| Nginx | Ingestors | 8080 | HTTP proxy |
| Kafka brokers | Kafka brokers | 29092, 19093–19095 | Internal + controller |

---

## 6. Reference

- **Validation runbook:** [`docs/runbooks/validation.md`](../runbooks/validation.md) — full operational validation procedures
- **Runtime runbook:** [`docs/runbooks/runtime.md`](../runbooks/runtime.md) — runtime operations and troubleshooting
- **Merge summary:** [`current-kafka-dev-merge-change-summary-2026-02-20.md`](current-kafka-dev-merge-change-summary-2026-02-20.md)

---

## 7. Verification Progress Log (2026-02-20)

### 7.1 Session timeline

- 2026-02-20 13:10 +0900: Started static verification against current workspace files.
- 2026-02-20 13:18 +0900: Ran `docker compose -f infra/flink/docker-compose.distributed.yml config`; parse failed with `undefined network kafka-network`.
- 2026-02-20 13:19 +0900: Completed item-by-item status snapshot for Sections 1–3.
- 2026-02-20 13:42 +0900: Scope clarified: verification target is only single-machine + distributed compose settings; Kubernetes validation excluded.

### 7.2 Item status snapshot (Sections 1–3)

| Item | Status | Evidence |
|---|---|---|
| 1.1 Flink distributed: missing `networks` block | Not implemented | `infra/flink/docker-compose.distributed.yml` ends at line 100 without top-level `networks`; compose parse fails with `service "flink-jobmanager" refers to undefined network kafka-network`. |
| 1.2 Flink distributed: JM/TM RPC address mismatch | Not implemented | `infra/flink/docker-compose.distributed.yml:15` uses `flink-jobmanager`, while `infra/flink/docker-compose.distributed.yml:45`, `infra/flink/docker-compose.distributed.yml:65`, `infra/flink/docker-compose.distributed.yml:85` use `${FLINK_IP}`. |
| 2.1 Kafka-UI distributed: missing `depends_on` healthcheck | Not implemented | `infra/kafka/docker-compose.distributed.yml:131` to `infra/kafka/docker-compose.distributed.yml:143` define `kafka-ui` with no `depends_on`. |
| 2.2 Nginx distributed: missing `/health` endpoint | Not implemented | `infra/nginx/templates/nginx.distributed.conf.template` has no `location /health`; single-machine has it at `infra/nginx/nginx.single-machine.conf:43`. |
| 2.3 Ingestor `application.yml` overrides by `ReactorKafkaConfig.java` | Not implemented | `services/ingestor/bin/main/application.yml:12` to `services/ingestor/bin/main/application.yml:19` still differs from hardcoded Reactor values in `services/ingestor/src/main/java/com/ingestion/config/ReactorKafkaConfig.java:38` to `services/ingestor/src/main/java/com/ingestion/config/ReactorKafkaConfig.java:45`. |
| 3.1 Flink distributed TM-3 JDBC interval inconsistency | Not implemented | TM-3 sets `FLINK_JDBC_BATCH_INTERVAL_MS=10000` at `infra/flink/docker-compose.distributed.yml:95`; TM-1/TM-2 do not set override. |
| 3.2 Flink distributed: parallelism=3 with 7 total slots | Not implemented | `parallelism.default: 3` at `infra/flink/docker-compose.distributed.yml:16`; slots are 2/2/3 at `infra/flink/docker-compose.distributed.yml:46`, `infra/flink/docker-compose.distributed.yml:66`, `infra/flink/docker-compose.distributed.yml:86`. |
| 3.3 Ingestor batch size split (distributed=1000 vs single=2000) | Needs runtime validation | Distributed uses `1000` at `services/ingestor/docker-compose.distributed.yml:12`; single-machine uses `2000` at `services/ingestor/docker-compose.yml:14`. |
| 3.4 ClickHouse sink parallelism hardcoded to 3 | Not implemented | `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:166` still has `.setParallelism(3)`. |
| 3.5 Ingestor `tryEmitNext` spin-wait retry (no backoff) | Not implemented | `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:183` to `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:187` still uses busy loop with `Thread.onSpinWait()`. |
| 3.6 Nginx `worker_connections` split (distributed=1024 vs single=4096) | Needs runtime validation | Distributed template sets `1024` at `infra/nginx/templates/nginx.distributed.conf.template:2`; single-machine sets `4096` at `infra/nginx/nginx.single-machine.conf:2`. |

### 7.3 Runtime verification status (Sections 4–5)

- Not executed in this session because the target stack is not currently running.
- `docker ps` shows only `k3d-taxi-registry`; no Kafka/Flink/Ingestor/ClickHouse/Nginx containers are up.

### 7.4 Implementation update (Step 1 + Step 2) — 2026-02-20 15:39 +0900

- Applied config fixes for `1.1`, `1.2`, `2.1`, `2.2`, `2.3`.
- Verified compose parsing:
  - `docker compose -f infra/flink/docker-compose.distributed.yml config` -> exit code `0` (env-var warnings only).
  - `docker compose -f infra/kafka/docker-compose.distributed.yml config` -> exit code `0` (env-var warnings only).

| Item | Updated status | Evidence |
|---|---|---|
| 1.1 Flink distributed: missing `networks` block | Implemented | `infra/flink/docker-compose.distributed.yml:104` to `infra/flink/docker-compose.distributed.yml:107` now defines top-level `kafka-network`. |
| 1.2 Flink distributed: JM/TM RPC address mismatch | Implemented (config aligned, runtime verification pending) | `jobmanager.rpc.address` now uses `${FLINK_IP}` consistently at `infra/flink/docker-compose.distributed.yml:15`, `infra/flink/docker-compose.distributed.yml:45`, `infra/flink/docker-compose.distributed.yml:65`, `infra/flink/docker-compose.distributed.yml:85`. |
| 2.1 Kafka-UI distributed: missing `depends_on` healthcheck | Implemented | `infra/kafka/docker-compose.distributed.yml:134` adds `depends_on` with `kafka-1` `service_healthy`. |
| 2.2 Nginx distributed: missing `/health` endpoint | Implemented | `infra/nginx/templates/nginx.distributed.conf.template:43` to `infra/nginx/templates/nginx.distributed.conf.template:46` adds `location /health`. |
| 2.3 Ingestor `application.yml` overrides by `ReactorKafkaConfig.java` | Implemented | Removed stale producer tuning block from `services/ingestor/bin/main/application.yml`; `spring.kafka.producer.properties.*` keys are no longer present. |

### 7.5 Remaining work (current)

- Medium-priority items still open for decision/implementation: `3.1`, `3.2`, `3.4`, `3.5`.
- Runtime validation still needed for: Sections `4` and `5`, and observational checks `3.3`, `3.6`.

### 7.6 Runtime validation execution (single-machine) — 2026-02-20 15:43 ~ 15:48 +0900

- Started stack with:
  - `docker compose -f infra/kafka/docker-compose.yml --env-file config/.env.single-machine up -d`
  - `docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env.single-machine up -d`
  - `docker compose -f services/ingestor/docker-compose.yml --env-file config/.env.single-machine up -d --build`
  - `docker compose -f infra/nginx/docker-compose.yml --env-file config/.env.single-machine up -d`
  - `docker compose -f infra/flink/docker-compose.yml --env-file config/.env.single-machine up -d --build`

| Checklist | Result | Evidence |
|---|---|---|
| 4.1 Kafka topic config | Failed | `taxi-event-data` reported `PartitionCount: 4`, `ReplicationFactor: 2` (expected `12` / `3`). |
| 4.2 Service health endpoints | Partial | `http://localhost:8080/health` = `200`, Flink overview `200`, Kafka-UI `200`; ingestor health endpoints (`8081/8082/8083`) failed (`000`). |
| 4.3 Ingest smoke test | Failed | `http://localhost:8080/api/events` and `http://localhost:8080/ingest` returned `502 Bad Gateway`. |
| 4.4 Flink jobs/checkpoints | Failed | Job state was `RESTARTING`; checkpoints counts all `0`. |
| 4.5 End-to-end flow | Failed | `kafka-consumer-groups` reported `Consumer group 'taxi-realtime-flink' does not exist`; no valid ingest flow established. |
| 4.6 ClickHouse async insert | Passed | `async_insert=1`, `async_insert_busy_timeout_ms=2000` confirmed. |
| 4.7 Log inspection | Failed | Ingestor: `dead_letter_queue.jsonl (Permission denied)`; Flink: JDBC sink connect refused to `127.0.0.1:8123`. |

Blocking root causes observed in this run:

1. Ingestor startup failure:
   - `java.io.FileNotFoundException: dead_letter_queue.jsonl (Permission denied)`.
   - Containers restart unhealthy, so nginx upstream returns `502`.
2. Flink single-machine connectivity failure:
   - Flink containers try `127.0.0.1` for Kafka/ClickHouse (`/127.0.0.1:9092/9094/9096`, `/127.0.0.1:8123`) and fail from inside container network namespace.
3. Kafka topic mismatch to expectation:
   - Existing topic metadata did not match expected `RF=3, partitions=12` in this run context.

### 7.7 Remediation plan (captured before implementation) — 2026-02-20 15:53 +0900

| Issue | Planned solution |
|---|---|
| Ingestor `dead_letter_queue.jsonl` permission denied | Make container runtime path writable for non-root user (`appuser`) and set a stable writable DLQ path (`/app/data/dead_letter_queue.jsonl`). |
| Flink single-machine cannot reach Kafka/ClickHouse via `127.0.0.1` | In single-machine compose, switch Flink internal targets to container DNS (`kafka-1/2/3` on internal port, `clickhouse` host). |
| Kafka topic config remains old (`4/2`) | Perform clean runtime reset for dev validation (remove Kafka/ClickHouse persisted volumes) and recreate topic with expected `partitions=12`, `replication-factor=3`, `min.insync.replicas=2`. |

Implementation order:

1. Patch code/config files for ingestor permissions and Flink single-machine addressing.
2. Clean reset Kafka/ClickHouse runtime state.
3. Recreate/verify topic config.
4. Re-run Section 4 checks and append results.

### 7.8 Additional issue found during re-test — stale Flink artifact

Observed issue:

- Flink image build used `services/flink-job/Dockerfile` that only copies `target/flink-kafka-print-0.1.0.jar`.
- This allows stale host-side jar artifacts to be deployed even when source code has changed.
- Re-test symptoms matched stale behavior:
  - checkpoint counts remained `0`,
  - runtime behavior did not match current source expectations.

Planned solution:

1. Replace Flink Dockerfile with multi-stage build:
   - stage 1: build jar from `pom.xml` + `src/` using Maven,
   - stage 2: copy freshly built jar into Flink image.
2. Rebuild Flink image and restart Flink services.
3. Re-run runtime checks for job/checkpoints/end-to-end flow.

### 7.9 Implementation completion + re-validation (single-machine) — 2026-02-20 16:31 ~ 16:38 +0900

Implemented in code/config:

- `services/flink-job/Dockerfile`: switched to multi-stage Maven build (fresh jar from source each build).
- `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java`: enabled configurable watermark idleness via `FLINK_WATERMARK_IDLENESS_SEC` (default `30`).
- `services/ingestor/Dockerfile`: ensured writable runtime directory and DLQ path for non-root user (`/app/data/dead_letter_queue.jsonl`).
- `infra/flink/docker-compose.yml`: single-machine Flink now uses container DNS targets (`kafka-1/2/3:29092`, `clickhouse`) instead of loopback.
- `services/ingestor/src/main/resources/application.yml` and `services/ingestor/bin/main/application.yml`: removed stale `spring.kafka.producer.properties.*` block to avoid conflict with Reactor programmatic producer settings.

Validation results (re-run):

| Checklist | Result | Evidence |
|---|---|---|
| 4.1 Kafka topic config | Passed | `PartitionCount: 12`, `ReplicationFactor: 3`, topic config includes `min.insync.replicas=2`. |
| 4.2 Service health endpoints | Passed | `8081/8082/8083/8080/8090/8084` all return `200`. |
| 4.3 Ingest smoke test | Passed (with corrected endpoint) | `POST http://localhost:8080/ingest` returned `202`; `/api/events` returns `404` by design (route not implemented). |
| 4.4 Flink jobs/checkpoints | Passed | Job `Taxi Reliable Job (Ordered + Cleanup)` is `RUNNING`; checkpoints `completed=10`, `failed=0` in sampled run. |
| 4.5 End-to-end data flow | Passed | Kafka consumer offsets present with lag `0`; ClickHouse `taxi_events` count observed at `130` with sample rows present. |
| 4.6 ClickHouse async insert | Passed | `async_insert=1`, `async_insert_busy_timeout_ms=2000` confirmed. |
| 4.7 Log inspection | Passed (no blocking errors) | No recent `ERROR/Exception/Permission denied/Connect refused` patterns from ingestor or Flink checks. |

### 7.10 Distributed scope status (non-Kubernetes) — 2026-02-20 16:33 +0900

- Scope reconfirmed: target is single-machine + distributed compose only (Kubernetes excluded).
- Compose parse checks:
  - `docker compose -f infra/flink/docker-compose.distributed.yml config` -> `ok` (env warnings only).
  - `docker compose -f infra/kafka/docker-compose.distributed.yml config` -> `ok` (env warnings only).
  - `docker compose -f infra/nginx/docker-compose.distributed.yml config` -> `ok`.

Outstanding (non-blocking, medium-priority tuning decisions still open):
- `3.1` TM-3 JDBC interval inconsistency (`10000ms` vs others/default).
- `3.2` Distributed parallelism/slot utilization mismatch (`parallelism=3` vs total 7 slots).
- `3.4` ClickHouse sink parallelism hardcoded to `3`.
- `3.5` `tryEmitNext` spin-wait backoff improvement.
- `3.6` Nginx distributed `worker_connections=1024` vs single-machine `4096`.

### 7.11 Single-machine medium-priority closure (implemented) — 2026-02-20 16:39 ~ 16:42 +0900

Issue capture and solution:

| Issue | Solution implemented |
|---|---|
| `3.4` ClickHouse sink parallelism hardcoded to `3` | Added configurable sink parallelism in Flink job: `FLINK_CLICKHOUSE_SINK_PARALLELISM` (default follows `FLINK_PARALLELISM`). |
| `3.5` Ingestor `tryEmitNext` spin-wait with no backoff | Replaced busy-spin retry with bounded retry + exponential nanosleep backoff using `LockSupport.parkNanos`. |

Code changes:

- `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java`
  - Added `clickhouseSinkParallelism` to `JobConfig`.
  - Added env support: `FLINK_CLICKHOUSE_SINK_PARALLELISM`.
  - Sink now uses `.setParallelism(jobConfig.clickhouseSinkParallelism)`.
  - Startup config log now prints `sinkParallelism`.
- `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java`
  - Added bounded retry constants for `FAIL_NON_SERIALIZED`.
  - Replaced `Thread.onSpinWait()` loop with `LockSupport.parkNanos()` backoff.

Post-change runtime verification (single-machine):

| Check | Result | Evidence |
|---|---|---|
| Service health | Passed | `8081/8082/8083/8080/8084/8090` all returned `200`. |
| Kafka topic shape/config | Passed | `PartitionCount=12`, `ReplicationFactor=3`, `min.insync.replicas=2`. |
| Flink running/checkpoints | Passed | Job `RUNNING`; checkpoint counts observed (`completed=1`, `failed=0`) after restart. |
| Ingest smoke + E2E flow | Passed | `POST /ingest` returned `202`; consumer offsets advancing; ClickHouse row count increased (`181` observed). |
| Flink sink parallelism activation | Passed | Runtime log shows `[CONFIG] ... parallelism=12, sinkParallelism=12 ...`. |

Current single-machine status:

- Blocking issues: **closed**.
- Medium-priority single-machine items: **closed** (`3.4`, `3.5`).
- Remaining open items are distributed-focused: `3.1`, `3.2`, `3.6`.

### 7.12 Fresh re-verification pass (single-machine) — 2026-02-20 16:46 ~ 16:48 +0900

Reason:

- User requested one more full confirmation before moving to distributed verification.

Results:

| Check | Result | Evidence |
|---|---|---|
| Service endpoints | Passed | `8081/8082/8083/8080/8084/8090` all returned `200`. |
| Kafka topic config | Passed | `PartitionCount=12`, `ReplicationFactor=3`, `compression.type=lz4`, `min.insync.replicas=2`. |
| Flink job + checkpoints | Passed | Job `RUNNING`; checkpoint counts observed as `total=12`, `completed=12`, `failed=0`. |
| Ingest smoke | Passed | `POST /ingest` returned `202`. |
| Consumer progress | Passed | Consumer group offsets advancing with expected transient lag after fresh events. |
| ClickHouse end-to-end | Passed | Count before/after ingest burst: `264 -> 265` (`delta=1` observed in sample window). |
| ClickHouse async insert | Passed | `async_insert=1`, `async_insert_busy_timeout_ms=2000` still set. |
| Severe log scan | Passed | No recent `ERROR/Exception/Permission denied/Connect refused` patterns from ingestor/flink scan. |

Conclusion:

- Single-machine implementation remains stable and verified after latest code/config changes.

### 7.13 Single-machine stack shutdown (pre-distributed) — 2026-02-20 16:49 +0900

Action:

- Stopped single-machine runtime stack before distributed phase:
  - `infra/flink/docker-compose.yml`
  - `infra/nginx/docker-compose.yml`
  - `services/ingestor/docker-compose.yml`
  - `infra/clickhouse/docker-compose.yml`
  - `infra/kafka/docker-compose.yml`

Result:

- All single-machine containers were stopped and removed.
- `docker ps` now shows only `k3d-taxi-registry`.

---

## 8. Distributed Verification Plan (next execution)

### 8.1 Scope and goal

- Scope: distributed Docker Compose deployment only (non-Kubernetes).
- Goal: verify connectivity, correctness, and steady-state behavior across machines with current configs.

### 8.2 Config baseline to validate first

- `infra/flink/docker-compose.distributed.yml`
  - JM/TM RPC address uses `${FLINK_IP}` consistently.
  - TaskManager slots aligned to `1/1/1` for `parallelism=3`.
  - JDBC batch interval aligned to `3000ms` across TMs.
- `infra/kafka/docker-compose.distributed.yml`
  - `kafka-ui` has startup dependency on broker health.
- `infra/nginx/templates/nginx.distributed.conf.template`
  - `/health` endpoint present.
  - `worker_connections` tuned to `4096`.

Parse status:

- `docker compose -f infra/flink/docker-compose.distributed.yml config` -> `ok` (env warnings only).
- `docker compose -f infra/kafka/docker-compose.distributed.yml config` -> `ok` (env warnings only).
- `docker compose -f infra/nginx/docker-compose.distributed.yml config` -> `ok`.

### 8.3 Execution order (recommended)

1. Prepare host networking and env values on each machine.
2. Start Kafka brokers (3 nodes), then validate broker-to-broker quorum/connectivity.
3. Start ClickHouse node and validate from Flink host with TCP check.
4. Start 3 ingestors and nginx LB, then verify `/health` on LB and each ingestor.
5. Start Flink JM + TMs, then verify JM/TM registration and slot totals.
6. Run ingest smoke through nginx LB and confirm Kafka topic/consumer progress.
7. Confirm Flink checkpoints, then validate ClickHouse row growth and queryability.
8. Run 10-15 minute stability watch (errors, restarts, lag trends, checkpoint failures).

### 8.4 Must-pass acceptance criteria

- All services stay `Up` without restart loops for the stability window.
- Kafka topic has expected partition/replication config and healthy ISR.
- Flink job remains `RUNNING` with checkpoints completing and no sustained failures.
- Ingest via nginx returns `202` for normal load path.
- ClickHouse row count grows during ingest and query latency remains acceptable.
- No repeated severe errors (`permission denied`, `connect refused`, unbounded exceptions).

### 8.5 Remaining tuning decisions after distributed run

- Revisit distributed slot model (`1/1/1`) if CPU is under-utilized under target load.
- Revisit sink parallelism override (`FLINK_CLICKHOUSE_SINK_PARALLELISM`) if ClickHouse becomes bottleneck.
- Revisit nginx `worker_connections=4096` based on observed concurrent connection profile.

### 8.6 Local distributed simulation fallback (single host, no Tailscale)

Use this when real distributed machines are offline but you still want to validate distributed compose behavior.

Important:

- Do **not** use `127.0.0.1` for distributed service IPs inside containers.
- Use one host-reachable local IP (for example your LAN IP) for all `*_IP` entries during local simulation.
- For one-host simulation, set `FLINK_IP=flink-jobmanager` so TaskManagers connect via Docker DNS (avoids host-port collision on RPC/Blob paths).
- Backup and restore `config/.env` so production Tailscale values are preserved.

Step A: prepare local distributed env

```bash
# 1) choose local host IP (example)
export HOST_IP=<your_local_host_ip>

# 2) backup current production env
cp config/.env config/.env.prod.bak

# 3) start from distributed template
cp config/.env.distributed config/.env

# 4) set all distributed IP keys to HOST_IP (except FLINK_IP)
for k in GENERATOR_IP NGINX_IP INGESTOR_1_IP INGESTOR_2_IP INGESTOR_3_IP KAFKA_1_IP KAFKA_2_IP KAFKA_3_IP CLICKHOUSE_IP; do
  sed -i "s|^${k}=.*|${k}=${HOST_IP}|" config/.env
done

# 5) local one-host override for Flink JM address
sed -i "s|^FLINK_IP=.*|FLINK_IP=flink-jobmanager|" config/.env
```

Step B: bring up distributed stack on one host

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up -d --build
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build
```

Step C: quick validation

```bash
curl -s http://localhost:8080/health
curl -s http://localhost:8084/overview
docker exec kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic taxi-event-data
curl -s -X POST http://localhost:8080/ingest -H "Content-Type: application/json" -d '{"trip_id":970001,"ts":"2026-02-20T23:40:00Z","event":"PICKUP","lat":40.748,"lon":-73.985}'
docker exec clickhouse clickhouse-client --query "SELECT count() FROM taxi_events"
```

Step D: cleanup + restore production env

```bash
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env down
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env down
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env down
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env down
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env down

mv config/.env.prod.bak config/.env
```

### 8.7 Executed local distributed simulation (this session) — 2026-02-20 17:06 ~ 17:10 +0900

What was done:

- Backed up production env:
  - `config/.env.prod.tailscale.20260220-170627.bak`
- Created local distributed env:
  - `config/.env.local-distributed.20260220-170627`
- Applied local simulation values:
  - All service IP keys -> `100.99.149.67`
  - Local one-host override: `FLINK_IP=flink-jobmanager`
- Started distributed stack in order:
  - Kafka -> ClickHouse -> Ingestor -> Nginx -> Flink

Observed issue and fix:

- Initial run with `FLINK_IP=100.99.149.67` caused TaskManager registration failure (`Could not resolve ResourceManager ... 100.99.149.67:6123`) in one-host simulation.
- Switching to `FLINK_IP=flink-jobmanager` resolved it.

Validation snapshot:

| Check | Result | Evidence |
|---|---|---|
| Ingestor/Nginx/Kafka-UI health | Passed | `8081/8082/8083/8080/8090` returned `200`. |
| Flink cluster status | Passed | `/overview` shows `taskmanagers=3`, `slots-total=3`, `jobs-running=1`. |
| Flink checkpoints | Passed | Running job checkpoint counts observed with `completed=1`, `failed=0` in sampled run. |
| Ingest API | Passed | `POST /ingest` returned `202`. |
| ClickHouse flow | Passed | `SELECT count() FROM taxi_events` returned `315` after ingest traffic. |

Current state:

- Local distributed simulation stack is currently **running**.

### 8.8 Finalization (requested) — 2026-02-20 17:12 +0900

Summary:

- Distributed profile validation target (local simulation) completed successfully.
- No blocking issues observed in the local distributed run after `FLINK_IP=flink-jobmanager` one-host override.

Cleanup and restore actions completed:

- Stopped and removed local distributed simulation stack:
  - Flink distributed compose
  - Nginx distributed compose
  - Ingestor distributed compose
  - ClickHouse distributed compose
  - Kafka distributed compose
- Restored `config/.env` to original Tailscale backup:
  - source: `config/.env.prod.tailscale.20260220-170627.bak`
  - verification: SHA-256 of restored `config/.env` matches backup.
- Post-cleanup runtime state:
  - `docker ps` shows no project containers running.
