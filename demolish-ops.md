---
status: IN PROGRESS
created: 2026-02-04
purpose: remove-ops-and-shift-runtime-control-to-infra-services
progress:
  - 2026-02-04: single-machine compose relocation started
  - 2026-02-04: component-owned single-machine compose files added
  - 2026-02-04: component-owned distributed compose files added
  - 2026-02-04: runtime YAML dependencies removed for Kafka/ClickHouse/Ingestor/Flink
  - 2026-02-04: ops directory deleted
  - 2026-02-04: root launcher compose files deleted
  - 2026-02-04: config + component runbook docs updated to per-component compose entrypoints
---

# Demolish Ops Plan

## Understanding of the change
1. Remove `ops/` from the repo.
2. Move Docker Compose ownership to each component directory under `infra/` and `services/`.
3. Stop using component runtime YAML files (`*/config/default.yaml`) for deployed services.
4. Keep `config/.env` as the only shared source for network values (`*_IP`, `*_PORT`).
5. Hardcode non-network runtime values directly in compose `environment` blocks.

## Current state (from repo scan)
1. Compose entrypoints are centralized in `ops/compose/{single-machine,distributed}` and split across many files.
2. Runtime YAML is currently used by Kafka, ClickHouse, Ingestor, Flink (and Generator natively).
3. Some distributed network wiring is already known to be broken (Kafka network declaration, Flink network isolation, missing ingestor networks).
4. `version: "3.8"` is still present in compose files and should be removed during migration.

## Target structure
1. Component-owned compose files:
   - `infra/kafka/docker-compose.yml`
   - `infra/kafka/docker-compose.distributed.yml`
   - `infra/clickhouse/docker-compose.yml`
   - `infra/clickhouse/docker-compose.distributed.yml`
   - `infra/nginx/docker-compose.yml`
   - `infra/nginx/docker-compose.distributed.yml`
   - `services/ingestor/docker-compose.yml`
   - `services/ingestor/docker-compose.distributed.yml`
   - `infra/flink/docker-compose.yml`
   - `infra/flink/docker-compose.distributed.yml`
2. `ops/` deleted after cutover.

## Config model after cutover
1. Keep:
   - `config/.env.single-machine`
   - `config/.env.distributed`
   - `config/.env` (active copy)
2. Remove runtime YAML for deployed components:
   - `infra/kafka/config/default.yaml`
   - `infra/clickhouse/config/default.yaml`
   - `services/ingestor/config/default.yaml`
   - `services/flink-job/config/default.yaml`
3. Replace YAML values with compose environment values:
   - Kafka tuning keys (`KAFKA_*`) defined directly in broker services.
   - Ingestor app keys (`APP_KAFKA_TOPIC`, `APP_TUNING_*`) defined in ingestor services.
   - Flink job keys (`FLINK_KAFKA_TOPIC`, `FLINK_CLICKHOUSE_DATABASE`, `FLINK_CLICKHOUSE_TABLE`, tuning) defined in Flink services.
   - ClickHouse runtime tuning handled directly in compose (or dropped if defaults are acceptable).

## Execution plan

### Phase 1: Compose relocation (no behavior change yet)
1. Copy existing compose definitions from `ops/compose/...` into component directories.
2. Keep service names, network names, volumes, and env references identical.
3. Validate `docker compose ... config` for both modes.

### Phase 2: Remove YAML runtime config dependency
1. Kafka:
   - Remove `infra/kafka/entrypoint.sh` usage and YAML mount.
   - Inline broker tuning env keys in compose.
2. ClickHouse:
   - Remove `infra/clickhouse/config/default.yaml` mount.
   - Keep needed runtime options directly in compose.
3. Ingestor:
   - Remove `/app/config/default.yaml` mount.
   - Remove `spring.config.import` dependency in `application.yml`.
   - Provide required runtime values through compose environment.
4. Flink:
   - Remove `/opt/flink/config/default.yaml` mount and `FLINK_CONFIG_PATH`.
   - Read all required runtime values from environment (small code refactor in `TaxiRealtimeJob.java`).

### Phase 3: Delete ops and dead config files
1. Delete `ops/`.
2. Delete now-unused runtime YAML config files listed above.
3. Remove stale docs and references to removed files/paths.

### Phase 4: Correctness and smoke tests
1. Fix distributed networking issues during compose rewrite:
   - Invalid Kafka `external + driver` network declarations.
   - Flink network reachability to Kafka/ClickHouse.
   - Missing ingestor network attachments.
2. Run config rendering checks:
   - single-machine `docker compose ... config`
   - distributed `docker compose ... config`
3. Run minimal startup smoke tests for both modes and confirm:
   - Kafka healthy
   - Ingestor healthy
   - Flink can consume Kafka and write ClickHouse
   - Nginx routes to ingestors

## Acceptance criteria
1. `ops/` directory no longer exists.
2. No compose file mounts `*/config/default.yaml`.
3. No runtime code path depends on service YAML for deployed services.
4. `config/.env*` contains only IP/PORT keys.
5. Updated docs point to new compose paths and commands.
6. Single-machine and distributed smoke tests pass.

## Decisions made
1. Generator scope:
   - Keep `services/generator/config/default.yaml` for native CLI runs.
2. Root launcher strategy:
   - Remove root launcher files and run component compose files directly.
3. Distributed deployment style:
   - Use one distributed file per component containing all instances.
