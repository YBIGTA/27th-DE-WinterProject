---
status: IN PROGRESS
created: 2026-02-04
pipeline: generator → nginx → ingestor → kafka → (s3 sink connector → S3) / (flink → clickhouse)
---

# Config Upgrade Plan

## What Changes and Why

The config system currently only covers `generator → nginx → ingestor → kafka`.
We need to extend it to the full pipeline and introduce an **instance registry** pattern
in the distributed template so it's clear who runs what before deployment.

### Two versions (kept)
- **`.env.single-machine`** — local, all Docker, everyone just runs it
- **`.env.distributed`** — multi-machine, instance registry at top maps roles → IPs

---

## Instance Registry (distributed only)

The top of `.env.distributed` defines every machine's role and IP in one place.
The registry is the **single source of truth** for IPs — update an IP there and it
flows through automatically to all components via Docker Compose `${VAR}` substitution
in each component's `environment:` block.

**How it works:**
- `.env.distributed` has the registry IPs + constants (topic, ports, db/table names)
- Each component's `docker-compose` file reads the `.env` via `env_file:`, then derives
  its own connection strings in `environment:` using `${KAFKA_1_IP}`, `${CLICKHOUSE_IP}`, etc.
- Docker Compose expands `${VAR}` in `environment:` using values from `env_file` — this is
  native Docker behavior, no custom tooling needed.

**Exceptions (no automatic derivation):**
- **Generator (C++):** Only reads `INGEST_URL`. Nginx is co-located → always `localhost:8080`. No derivation needed.
- **Nginx upstream IPs:** Nginx can't read env vars in `upstream {}` blocks. `nginx.distributed.conf` must still be manually edited.
- **`.env` file values themselves:** The C++ parser does NOT expand `${VAR}` inside `.env` values. Derivation happens in compose files, not in the `.env` file.

```
GENERATOR_IP        → Machine A  (co-located with Nginx)
NGINX_IP            → Machine A
INGESTOR_1_IP       → Machine B
INGESTOR_2_IP       → Machine C
INGESTOR_3_IP       → Machine D
KAFKA_1_IP          → Machine E
KAFKA_2_IP          → Machine F
KAFKA_3_IP          → Machine G
FLINK_IP            → Machine H  (single node: JobManager + TaskManager)
CLICKHOUSE_IP       → Machine I
(no S3 connector    → runs on AWS)
```

---

## Files to Modify

| # | File | What | Status |
|---|------|------|--------|
| 1 | `config/.env.single-machine` | Rewrite: fix 3-broker kafka, add clickhouse + flink vars | Done |
| 2 | `config/.env.distributed` | Strip redundant derived vars → keep registry IPs + constants only | TODO: strip |
| 3 | `config/EXPLANATION.md` | Update docs: new pipeline, registry pattern, expanded tables | Done (needs re-check after strip) |
| 4 | `config/README.md` | Update: one-step IP update procedure, nginx manual-edit note | Done (needs re-check after strip) |
| 5 | `ingestor/docker-compose.ingestor-{1,2,3}.yml` | Add `environment:` deriving `SPRING_KAFKA_BOOTSTRAP_SERVERS` from `${KAFKA_*_IP}` | TODO |
| 6 | `infra/kafka/docker-compose.kafka-{1,2,3}.yml` | Update `KAFKA_ADVERTISED_LISTENERS` + `KAFKA_CONTROLLER_QUORUM_VOTERS` to use `${KAFKA_*_IP}` | TODO |
| 7 | `infra/kafka/docker-compose.kafka-ui.yml` | Add `env_file` + derive bootstrap servers from `${KAFKA_*_IP}` | TODO |

---

## Checklist

- [x] Rewrite `.env.single-machine`
  - [x] Fix `SPRING_KAFKA_BOOTSTRAP_SERVERS` → `kafka-1:29092,kafka-2:29092,kafka-3:29092`
  - [x] Add ClickHouse section (`CLICKHOUSE_HOST`, `HTTP_PORT`, `NATIVE_PORT`, `DATABASE`, `TABLE`)
  - [x] Add Flink section (`FLINK_KAFKA_BOOTSTRAP_SERVERS`, `FLINK_CLICKHOUSE_*`, ports)
  - [x] Add S3 connector comment block (runs on AWS, points to `connectors/` template)
- [ ] Strip `.env.distributed` — remove redundant derived vars, keep only:
  - Instance registry IPs
  - Constants: `APP_KAFKA_TOPIC`, ports (`KAFKA_INTERNAL_PORT`, `KAFKA_CONTROLLER_PORT`, `NGINX_LB_PORT`, `INGESTOR_PORT`, `KAFKA_UI_PORT`, `FLINK_JOBMANAGER_PORT`, `FLINK_TASKMANAGER_SLOTS`), db/table (`CLICKHOUSE_DATABASE`, `CLICKHOUSE_TABLE`, `FLINK_CLICKHOUSE_DATABASE`, `FLINK_CLICKHOUSE_TABLE`, ports `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_NATIVE_PORT`, `FLINK_CLICKHOUSE_PORT`)
  - `INGEST_URL` (generator only reads this; always localhost — nginx co-located)
  - Remove: `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_BOOTSTRAP_SERVERS_*`, `NGINX_UPSTREAM_*`, `KAFKA_ADVERTISED_LISTENERS_*`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, `KAFKA_UI_BOOTSTRAP_SERVERS`, `FLINK_KAFKA_BOOTSTRAP_SERVERS`, `FLINK_CLICKHOUSE_HOST`, `CLICKHOUSE_HOST`, `KAFKA_{1,2,3}_EXTERNAL_PORT`
- [ ] Update per-ingestor compose files (`ingestor/docker-compose.ingestor-{1,2,3}.yml`)
  - Add `environment:` block deriving `SPRING_KAFKA_BOOTSTRAP_SERVERS` and `APP_KAFKA_TOPIC` from `${KAFKA_*_IP}`
- [ ] Update per-broker kafka compose files (`infra/kafka/docker-compose.kafka-{1,2,3}.yml`)
  - Replace hardcoded `KAFKA_ADVERTISED_LISTENERS` with `${KAFKA_N_IP}` references
  - Replace hardcoded `KAFKA_CONTROLLER_QUORUM_VOTERS` with `${KAFKA_*_IP}` references
- [ ] Update kafka-ui compose (`infra/kafka/docker-compose.kafka-ui.yml`)
  - Add `env_file: ../../config/.env`
  - Derive `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` from `${KAFKA_*_IP}`
- [x] Update `EXPLANATION.md` (done — revisit after strip + compose changes)
- [x] Update `README.md` (done — revisit after strip + compose changes)

---

## Constraints (from code audit)

| Constraint | Source | Impact |
|------------|--------|--------|
| No `${VAR}` expansion in `.env` values | C++ `load_env_file()` parser | All values must be literal |
| No inline comments (`value # comment`) | Same parser | Comments on own lines only |
| Nginx can't read env vars in `upstream {}` | Nginx limitation | `nginx.distributed.conf` IPs must be manually edited |
| Local ingestor compose hardcodes kafka bootstrap | `ingestor/docker-compose.yml` `environment:` | `.env` value is for distributed + documentation |
| Per-broker kafka compose hardcodes advertised listeners | `docker-compose.kafka-{1,2,3}.yml` | `KAFKA_ADVERTISED_LISTENERS_*` vars are forward-looking |
| ClickHouse compose has no `env_file` | Self-contained | Vars in `.env` are for Flink, not clickhouse itself |
| Flink compose is empty | Not configured yet | All flink vars are placeholders |
| S3 connector is a JSON template deployed via REST on AWS | `connectors/s3-sink-config.template.json` | No env vars or machines needed |

---

## Verification (after implementation)

1. `grep` kafka bootstrap in `.env.single-machine` → must be `kafka-1:29092,kafka-2:29092,kafka-3:29092`
2. Instance registry in `.env.distributed` → 10 IPs present, all `127.0.0.1` as placeholder
3. `grep taxi-event-data` across both files → appears in `APP_KAFKA_TOPIC`, `FLINK_KAFKA_TOPIC`, and S3 comment
4. No line has `value # comment` pattern (inline comment check)
5. `EXPLANATION.md` references all components in the pipeline
