---
status: IN PROGRESS
created: 2026-02-04
references:
  - REFACTORING.md          # Phase 1 & 2 overview
  - config/COMPONENT-CONFIGS.md  # current config matrix
  - config/EXPLANATION.md        # runtime wiring detail
---

# Phase 2 Refinement — Config Loading & Cleanup

## Context

Phase 2 established the config split: `.env` holds network (IP/PORT),
component YAML holds tuning. Three components — Kafka, ClickHouse, Nginx —
can't read YAML natively, so the original implementation used inline `awk`
in each compose entrypoint to convert YAML → env vars.

That worked, but had concrete problems:

- **Kafka**: the same 5-line awk block was copy-pasted into every broker
  service — 6 copies total across single-machine + distributed.
- **Nginx**: the entire startup was one escaped `sh -c` string with a
  hardcoded `envsubst` variable list that had to be manually updated
  whenever a new tuning key was added.
- **ClickHouse**: used awk conversion when the image (23.8) natively
  supports YAML config files at `/etc/clickhouse-server/config.d/`.

---

## What Changed

### Kafka — awk extracted to one entrypoint script

- `infra/kafka/entrypoint.sh` owns the YAML → env conversion.
- All broker services (SM + DM) mount and run this single script.
- `infra/kafka/config/default.yaml` remains the source of truth for
  broker tuning params.

### ClickHouse — native YAML, zero env conversion

- `infra/clickhouse/config/default.yaml` changed from flat env-var style
  (`TZ: "Asia/Seoul"`) to ClickHouse's own config key (`timezone: "Asia/Seoul"`).
- Mounted directly to `/etc/clickhouse-server/config.d/`.
- Entrypoint override removed from both SM and DM compose files.

### Nginx — tuning values out of env entirely

**Single-machine**: `infra/nginx/nginx.single-machine.conf` is a fully
static `nginx.conf`. Upstream servers are Docker service names
(`ingestor-1:8080` etc.), tuning values are hardcoded. Compose is just
image + port + volume mount. No env, no template, no envsubst.

**Distributed**: `infra/nginx/templates/nginx.distributed.conf.template`
has tuning values hardcoded directly. Only the upstream IPs
(`${INGESTOR_1_IP}`, `${INGESTOR_2_IP}`, `${INGESTOR_3_IP}`,
`${INGESTOR_PORT}`) remain as env vars — those are network routing,
which belongs in `.env`. Nginx's own variables (`$host`, `$remote_addr`,
etc.) are escaped as `$$` so `envsubst` skips them. The compose command
is now a single clean line.

### Dead files removed

| File | Reason |
|---|---|
| `infra/nginx/config/default.yaml` | Values moved into conf files directly |
| `infra/nginx/config/` (directory) | Empty after above |
| `infra/nginx/templates/nginx.single-machine.conf.template` | Replaced by static conf |
| `ops/compose/single-machine/docker-compose.kafka-ui.yml` | kafka-ui already defined in `docker-compose.kafka.yml`; this was never included |

---

## Still TODO

### 1. Compose file consolidation
`ops/compose/` has 18 files. The top-level `docker-compose.yml` in each
mode just `include:`s the rest — the split files are never run directly.
Consolidate to **one `docker-compose.yml` per mode**. Mechanical merge,
no logic changes.

### 2. Dead infra directories
- `infra/flink/` — docs only. Flink runtime config is already in
  `services/flink-job/config/`.
- `infra/spark/` — single README. Nothing in the current pipeline uses
  Spark.

### 3. Network misconfigurations (distributed mode)
These will cause failures on `docker compose up` in distributed mode:

- `docker-compose.kafka-{1,2,3}.yml`: `kafka-network` declared with both
  `external: true` and `driver: bridge` — mutually exclusive, compose
  will reject.
- `docker-compose.flink.yml`: Flink services are on `flink-network` only.
  They can't reach Kafka (`kafka-network`) or ClickHouse (no network).
- `docker-compose.ingestor-{1,2,3}.yml`: no `networks:` key at all —
  ingestors can't reach Kafka.
- ClickHouse (both modes): no network declared — unreachable from Flink.

### 4. `version:` key in all compose files
`version: "3.8"` is obsolete in modern Docker Compose and generates
warnings. Straightforward removal once compose consolidation is done.

### 5. Ingestor healthcheck gate (single-machine)
`docker-compose.ingestor.yml` has a `kafka-healthcheck` service that is
just a busybox sleeping 5 seconds. Ingestors `depends_on` this, but it
doesn't actually wait for Kafka. Should depend directly on `kafka-1` with
`condition: service_healthy`.

### 6. CLUSTER_ID mismatch
Single-machine uses `MkU3OEVBNTcwNTJENDM2Qk`, distributed uses
`MkU3OEVCNTcwNTJENDM2Qk` (one character differs). Confirm whether this
is intentional or a copy-paste drift.

### 7. Smoke test
No end-to-end startup validation recorded for either mode after the config
changes above.
