# Loki + ML-Flink Merge Delta and Pipeline Evaluation Plan (2026-02-21)

## 1) Scope and Merge Targets

Checked recent merges on `dev`:

1. PR #17 `feat/ml-model`  
   Merge commit: `9304dfb`  
   Included commits:
   - `6448cfa` `feat(flink): add ONNX demand inference pipeline and ClickHouse prediction sink`
   - `168fbd8` `feat(model): add ONNX 2024 backtest workflow and document model pipeline`
   - `688f714` `wip: model updates before rebase`
   - `d98285a` `feat(model): Add initial structure and ignore data files`
   Delta size: `18 files changed, 2638 insertions, 18 deletions`

2. PR #16 `feat/loki`  
   Merge commit: `79f8b8a`  
   Included commit:
   - `a419165` `refactor:fix promtail setting`
   Delta size: `18 files changed, 461 insertions, 196 deletions`

---

## 2) What Was Added

### 2.1 PR #17 (`feat/ml-model`) additions

1. Flink job now has ONNX prediction path in addition to raw event sink:
   - Added `PredictionRow` and `OnnxPredictionProcessFunction`.
   - Added ONNX inference track from 3-minute demand windows to ClickHouse.
   - Window changed to tumbling 3-minute demand windows.
   - Files:
     - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java`
     - `services/flink-job/pom.xml` (`onnxruntime` dependency)

2. New ClickHouse prediction table:
   - `default.taxi_predictions(prediction_time, target_time, zone_id, predicted_demand, model_version)`
   - File: `infra/clickhouse/schema.sql`

3. Compose wiring for model serving:
   - Added model mount `../../model/models:/opt/flink/model:ro`
   - Added prediction env keys:
     - `FLINK_ENABLE_PREDICTION_SINK`
     - `FLINK_CLICKHOUSE_PREDICTION_TABLE`
     - `FLINK_ONNX_MODEL_PATH`
     - `FLINK_MODEL_VERSION`
     - `FLINK_MODEL_FEATURE_LAG_STEPS`
     - `FLINK_MODEL_HORIZON_STEPS`
     - `FLINK_MODEL_INTERVAL_MINUTES`
   - Files:
     - `infra/flink/docker-compose.yml`
     - `infra/flink/docker-compose.distributed.yml`

4. New model assets and docs:
   - `model/models/taxi_demand_model.onnx`
   - `model/notebooks/*.ipynb` (preprocess/train/backtest)
   - `model/README.md`, `model/EXPLANATION.md`
   - `.gitignore` updated to ignore model data directories

### 2.2 PR #16 (`feat/loki`) additions

1. Distributed compose files got component-local Promtail sidecars:
   - Kafka, ClickHouse, Flink, Grafana, Nginx, Prometheus, Ingestor
   - Files include:
     - `infra/kafka/docker-compose.distributed.yml`
     - `infra/clickhouse/docker-compose.distributed.yml`
     - `infra/flink/docker-compose.distributed.yml`
     - `infra/grafana/docker-compose.distributed.yml`
     - `infra/nginx/docker-compose.distributed.yml`
     - `infra/prometheus/docker-compose.distributed.yml`
     - `services/ingestor/docker-compose.distributed.yml`

2. Added per-component `promtail-config.yml` files:
   - `infra/*/promtail-config.yml`
   - `services/ingestor/promtail-config.yml`

3. Loki distributed files were changed:
   - `infra/loki/docker-compose.distributed.yml`
   - `infra/loki/promtail-config.distributed.yml`

4. Operational cleanup:
   - Removed `DISTRIBUTED-SETUP-TEMP.md`
   - `health-check.sh` mode changed to executable

---

## 3) High-Priority Merge Interaction Checks

These should be validated first because they can break runtime quickly:

1. `infra/flink/docker-compose.distributed.yml` now lacks model mount for `flink-taskmanager-3`.
   - Risk: ONNX model file may be missing on one TM; prediction operator may fail when scheduled there.

2. Distributed Promtail sidecars use `grafana/promtail:2.9.0` while Loki service is `grafana/loki:3.3.2`.
   - Risk: runtime incompatibility on Docker discovery API or log pipeline behavior.

3. Promtail URL path in distributed configs depends on `${LOKI_IP}:${LOKI_PORT}`.
   - Risk: missing/incorrect values in `config/.env` silently drop logs.

---

## 4) Pipeline Evaluation Plan (End-to-End)

### Phase A: Static Preflight

Run parse and artifact checks before boot:

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env config >/dev/null

test -f model/models/taxi_demand_model.onnx
rg -n "taxi_predictions" infra/clickhouse/schema.sql
```

### Phase B: Startup (Distributed Profile)

```bash
docker network create kafka-network || true

docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up -d --build
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env up -d
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env up -d
```

### Phase C: Data Pipeline Functional Checks

1. Health and ingress:

```bash
curl -sf "http://localhost:8080/health"
curl -s -o /tmp/ingest.code -w "%{http_code}" \
  -X POST "http://localhost:8080/ingest" \
  -H "Content-Type: application/json" \
  -d '{"trip_id":999001,"ts":"2026-02-21T00:00:00Z","event":"PICKUP","lat":40.758,"lon":-73.985}'
cat /tmp/ingest.code
```

2. Kafka consumption and Flink runtime:

```bash
curl -sf "http://localhost:8084/jobs/overview"
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --group taxi-realtime-flink --describe
```

3. ClickHouse raw + prediction sink checks:

```bash
docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_events"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_predictions"
docker exec clickhouse clickhouse-client --query "SELECT prediction_time,target_time,zone_id,predicted_demand,model_version FROM default.taxi_predictions ORDER BY prediction_time DESC LIMIT 20"
```

4. Prediction warm-up note:
   - Default `FLINK_MODEL_FEATURE_LAG_STEPS=20` and 3-minute windows.
   - First prediction per zone requires about 60 minutes of zone history.
   - `taxi_events` increasing while `taxi_predictions` stays 0 for early runtime can be expected.

### Phase D: Observability (Loki/Promtail) Checks

```bash
curl -sf "http://localhost:3100/ready"
docker ps --format '{{.Names}}' | rg 'promtail'
docker logs --tail 100 promtail-flink
docker logs --tail 100 promtail-kafka-1
docker logs --tail 100 promtail-loki
curl -G "http://localhost:3100/loki/api/v1/query" --data-urlencode 'query={job="docker"}'
```

### Phase E: Focused Regression Checks

1. Confirm ONNX path exists inside all Flink containers:

```bash
docker exec flink-jobmanager ls -l /opt/flink/model
docker exec flink-taskmanager-1 ls -l /opt/flink/model
docker exec flink-taskmanager-2 ls -l /opt/flink/model
docker exec flink-taskmanager-3 ls -l /opt/flink/model
```

2. Confirm no ONNX init failures:

```bash
docker logs --tail 200 flink-taskmanager-1 | rg -n "ONNX|Failed to initialize ONNX|Exception|ERROR"
docker logs --tail 200 flink-taskmanager-2 | rg -n "ONNX|Failed to initialize ONNX|Exception|ERROR"
docker logs --tail 200 flink-taskmanager-3 | rg -n "ONNX|Failed to initialize ONNX|Exception|ERROR"
```

---

## 5) Pass Criteria

All items below should pass to mark this merge set stable:

1. All distributed compose files parse with `--env-file config/.env`.
2. Ingest endpoint accepts events via Nginx (`/ingest` returns `202`).
3. Flink job remains running with checkpoints active.
4. `default.taxi_events` row count increases under load.
5. `default.taxi_predictions` starts increasing after lag warm-up window.
6. Loki is ready and Promtail containers stay up without persistent client errors.
7. No repeated ONNX initialization/runtime errors in Flink taskmanager logs.

---

## 6) Execution Log Template

- Date/Time:
- Env file used:
- Compose profile used:
- Checks passed:
- Checks failed:
- Evidence (command + output snippets):
- Follow-up actions:

## 7) Execution Log (2026-02-21)

- Date/Time: `2026-02-21 06:27:37 UTC` (`2026-02-21 15:27:37 KST`)
- Env file used: `config/.env`
- Compose profile used: distributed compose set (all files in Phase A/B)
- Checks passed:
  - Phase A static preflight: all 8 compose files parsed with `--env-file config/.env`
  - `model/models/taxi_demand_model.onnx` exists
  - `infra/clickhouse/schema.sql` contains `taxi_predictions` DDL
  - Phase B startup: Kafka/ClickHouse/Ingestor/Nginx/Flink/Loki/Prometheus/Grafana all `up -d` succeeded
  - `GET /health` via Nginx returned `200`
  - Flink overview endpoint returned `200` and job state `RUNNING`
  - Kafka consumer group `taxi-realtime-flink` offsets are advancing with `LAG=0`
  - `default.taxi_events` row count observed (`421`)
  - Loki readiness eventually `200 ready` after initial warm-up
- Checks failed:
  - `POST /ingest` via Nginx returned `504` (expected `202`)
  - `default.taxi_predictions` table missing in running ClickHouse (`UNKNOWN_TABLE`)
  - Promtail sidecars repeatedly fail docker discovery with Docker API mismatch (`client version 1.42 is too old`, daemon minimum `1.44`)
  - `flink-taskmanager-3` does not have `/opt/flink/model` mount
  - Flink taskmanager logs show repeated remote connection timeout to `${FLINK_IP}:6123` (`100.80.192.42:6123`) in this local run
- Evidence (command + output snippets):
  - `docker logs --tail 120 ingestor-lb`:
    - `upstream timed out ... upstream: "http://100.84.209.31:8081/ingest"`
    - `POST /ingest ... 504 ... upstream_addr="100.84.209.31:8081, 100.98.222.120:8083"`
  - `docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_predictions"`:
    - `DB::Exception: Table default.taxi_predictions does not exist. (UNKNOWN_TABLE)`
  - `docker exec flink-taskmanager-3 ls -l /opt/flink/model`:
    - `No such file or directory`
  - `docker logs --tail 200 promtail-flink`:
    - `Unable to refresh target groups ... client version 1.42 is too old. Minimum supported API version is 1.44`
  - `docker logs --tail 250 flink-taskmanager-{1,2,3}`:
    - repeated `ConnectTimeoutException ... /100.80.192.42:6123`
- Follow-up actions:
  - Add model volume mount to `flink-taskmanager-3` in `infra/flink/docker-compose.distributed.yml`
  - Reconcile distributed runtime addressing for this environment:
    - For local single-host test, use reachable upstreams in Nginx template/env (or use single-machine profile)
    - For true multi-machine test, execute compose per host with correct host-local service exposure
  - Ensure prediction table exists in current ClickHouse data volume:
    - apply `infra/clickhouse/schema.sql` manually or recreate volume in a clean run
  - Upgrade Promtail image to a version compatible with current Docker API/Loki stack (or pin Docker API compatibility)
  - Note: Loki log query check should use `query_range`; instant `query` with log selector returns `400` by API design

## 8) Remediation Plan (What to Fix, How)

### Priority 0 (Must fix first to unblock E2E)

1. Fix missing ONNX model mount on `flink-taskmanager-3`
   - Why: prediction operator can fail when scheduled on TM-3.
   - File:
     - `infra/flink/docker-compose.distributed.yml`
   - Change:
     - Add `volumes` mount on `flink-taskmanager-3`:
       - `../../model/models:/opt/flink/model:ro`
   - Verify:
     - `docker exec flink-taskmanager-3 ls -l /opt/flink/model`

2. Ensure ClickHouse prediction table exists in active runtime
   - Why: runtime currently returns `UNKNOWN_TABLE` for `default.taxi_predictions`.
   - Files:
     - `infra/clickhouse/schema.sql` (already contains DDL)
     - `docs/runbooks/runtime.md` (add explicit schema apply step for reused volumes)
   - Change:
     - Add runbook step to apply schema manually when volume is not fresh:
       - `docker exec clickhouse clickhouse-client --multiquery < /docker-entrypoint-initdb.d/schema.sql` or equivalent host-side query application.
   - Verify:
     - `docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM default LIKE 'taxi_predictions'"`
     - `docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_predictions"`

### Priority 1 (Fix distributed validation correctness)

3. Resolve `/ingest` 504 caused by unreachable distributed upstream IPs in local validation
   - Why: current `config/.env` points to remote `100.x.x.x`; local single-host test cannot reach those upstreams.
   - Files:
     - `config/.env` (runtime selection)
     - `infra/nginx/templates/nginx.distributed.conf.template` (uses `${INGESTOR_*_IP}`)
     - `docs/current/current-loki-ml-flink-merge-evaluation-plan-2026-02-21.md` (clarify execution mode)
   - Change:
     - Define two explicit validation modes:
       - Local distributed simulation (single-host fallback):
         - Start from `config/.env.distributed`
         - Set distributed `*_IP` keys to one host-reachable local IP (not `127.0.0.1`)
         - Set `FLINK_IP=flink-jobmanager` for one-host Docker DNS routing
         - Keep backup/restore flow for `config/.env`
       - Real distributed: run each compose on designated host with shared distributed `.env`.
     - Do not run distributed profile on one host with remote IP placeholders unless network-reachable by design.
   - Verify:
     - `curl -s -o /tmp/ingest.code -w "%{http_code}" -X POST http://localhost:8080/ingest ...` returns `202`.

4. Align Flink RPC addressing with execution topology
   - Why: repeated timeout to `${FLINK_IP}:6123` during current local distributed run.
   - Files:
     - `infra/flink/docker-compose.distributed.yml`
     - `config/.env`
   - Change:
     - For true distributed: keep host IP and ensure routing/firewall openness.
     - For local distributed simulation: apply one-host override (`FLINK_IP=flink-jobmanager`) and host-reachable `*_IP` values.
   - Verify:
     - No repeated `ConnectTimeoutException` in `flink-taskmanager-*` logs.

### Priority 2 (Observability stabilization)

5. Upgrade Promtail image version for Docker API compatibility
   - Why: `promtail:2.9.0` docker discovery client version is too old for current daemon API.
   - Files:
     - `infra/*/docker-compose.distributed.yml`
     - `services/ingestor/docker-compose.distributed.yml`
   - Change:
     - Bump Promtail tag to a version compatible with current Docker API and Loki stack.
     - Keep tags consistent across all component-local Promtail sidecars.
   - Verify:
     - `docker logs promtail-*` no longer shows `client version 1.42 is too old`.
     - `curl -G http://localhost:3100/loki/api/v1/query_range --data-urlencode 'query={job="docker"}' ...` returns non-empty streams after traffic.

### Execution Order

1. Patch `flink-taskmanager-3` model mount.
2. Apply/verify ClickHouse prediction table in running instance.
3. Normalize validation mode (local single-machine vs real distributed) and rerun Phase B/C.
4. Fix Promtail versions and rerun Phase D.
5. Re-run full Phase A~E checklist and update Section 7 with final pass/fail.

## 9) Remediation Execution Log (2026-02-21)

- Date/Time: `2026-02-21 06:45:27 UTC` (`2026-02-21 15:45:27 KST`)
- Scope: execute Section 8 fixes and revalidate Phase A~E under one-host distributed fallback.

### 9.1 Implemented Changes

1. Flink distributed TM-3 model mount fixed
   - File: `infra/flink/docker-compose.distributed.yml`
   - Change: added `../../model/models:/opt/flink/model:ro` to `flink-taskmanager-3`.

2. Promtail distributed version alignment
   - Files:
     - `infra/kafka/docker-compose.distributed.yml`
     - `infra/clickhouse/docker-compose.distributed.yml`
     - `services/ingestor/docker-compose.distributed.yml`
     - `infra/nginx/docker-compose.distributed.yml`
     - `infra/flink/docker-compose.distributed.yml`
     - `infra/loki/docker-compose.distributed.yml`
     - `infra/prometheus/docker-compose.distributed.yml`
     - `infra/grafana/docker-compose.distributed.yml`
   - Change: `grafana/promtail:2.9.0` -> `grafana/promtail:3.3.2`.

3. Kafka network compatibility adjustment for current stack wiring
   - File: `infra/kafka/docker-compose.distributed.yml`
   - Change: `kafka-network` changed to external network usage (`external: true`, `name: kafka-network`) to match other distributed compose files and avoid network label conflict in this run.

4. ClickHouse prediction schema applied to active runtime volume
   - Command: `docker exec -i clickhouse clickhouse-client --multiquery < infra/clickhouse/schema.sql`
   - Result: `default.taxi_predictions` table exists in active runtime.

### 9.2 One-Host Distributed Fallback Used

1. Backed up current env:
   - `config/.env.prod.tailscale.20260221-153644.bak`
2. Created one-host distributed env from `config/.env.distributed` with local override:
   - all distributed `*_IP` keys set to `100.99.149.67`
   - `FLINK_IP=flink-jobmanager`
3. After validation, restored original env and preserved local snapshot:
   - restored: `config/.env <- config/.env.prod.tailscale.20260221-153644.bak`
   - local snapshot: `config/.env.local-distributed.20260221-154523`
   - SHA-256 verified restored env equals backup.

### 9.3 Revalidation Results (Post-Fix)

1. Phase A static preflight
   - Result: pass for all distributed compose files and ONNX artifact presence.

2. Phase C data-path checks
   - `GET /health`: `200`
   - `POST /ingest`: `202` with valid event payload
   - Flink job: `RUNNING`
   - Checkpoints: `completed=4`, `failed=0` before restart; `completed=1`, `failed=0` after controlled Flink restart
   - `default.taxi_events`: increasing
   - `default.taxi_predictions`: table exists, current row count `0` (warm-up window not yet satisfied in sampled run)

3. Phase D observability checks
   - Loki readiness: `200 ready`
   - `query_range` (`{job="docker"}`): `200`, returned log streams
   - Promtail logs: no repeated Docker API mismatch signature (`client version ... too old`) after version alignment.

4. Phase E focused regressions
   - `/opt/flink/model` exists on JM/TM1/TM2/TM3
   - ONNX model load log observed on all TaskManagers
   - No new `UNKNOWN_TABLE` after schema application and Flink restart (previous errors were pre-fix history).

### 9.4 Residual Note

- `taxi_predictions` row growth remains pending warm-up/runtime data accumulation in this short validation window (expected with lag-step history requirement).

### 9.5 Follow-up Fix (Prometheus restart loop) — 2026-02-21

- Issue: `prometheus` restarted repeatedly with `prometheus: error: unexpected sh`.
- Fixes:
  1. `infra/prometheus/docker-compose.distributed.yml`
     - switched from inline `command: sh -c ...` to mounted `entrypoint.sh` execution (same pattern as single-machine compose).
  2. `infra/prometheus/entrypoint.sh`
     - added `${CLICKHOUSE_IP}` substitution for distributed template rendering completeness.
- Verification:
  - `docker ps`: `prometheus` is `Up` (no restart loop).
  - `curl http://localhost:9090/-/healthy`: `200`, `Prometheus Server is Healthy.`
  - Prometheus startup logs show successful config render/load and ready state.

### 9.6 Final Completion + Shutdown Record — 2026-02-21

1. Final completion check (one-host distributed fallback)
   - Runtime overrides used during final verification:
     - `FLINK_IP=flink-jobmanager`
     - `CLICKHOUSE_IP=clickhouse`
   - Result:
     - Flink job: `RUNNING` (`12/12` tasks)
     - `default.taxi_events`: `927`
     - `default.taxi_predictions`: `6` (`> 0` satisfied)
   - Sample prediction rows confirmed:
     - `prediction_time=2026-02-21 16:06:00`, `zone_id=230`, `predicted_demand=2.1256566`, `model_version=onnx_v1`

2. Runtime caveat confirmed
   - With restored production-style `config/.env` values, Flink can enter restart loops in one-host local run due to:
     - TaskManager RPC timeout to remote `${FLINK_IP}:6123`
     - JDBC timeout to remote `${CLICKHOUSE_IP}:8123`
   - For one-host local distributed validation, use local snapshot env or explicit runtime overrides above.

3. Stack shutdown completed (no volume deletion)
   - Executed `docker compose --env-file config/.env down` for:
     - `infra/grafana/docker-compose.distributed.yml`
     - `infra/prometheus/docker-compose.distributed.yml`
     - `infra/loki/docker-compose.distributed.yml`
     - `infra/flink/docker-compose.distributed.yml`
     - `infra/nginx/docker-compose.distributed.yml`
     - `services/ingestor/docker-compose.distributed.yml`
     - `infra/clickhouse/docker-compose.distributed.yml`
     - `infra/kafka/docker-compose.distributed.yml`
   - Final state:
     - `docker ps` empty (`NAMES STATUS PORTS` only header)
