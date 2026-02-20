# Loki Merge Validation Plan & Execution (2026-02-20)

Target merge:
- `f0a79ba` (`Merge branch 'feat/loki' into end2end-v2`)
- Parent-1: `57b59c0` (pre-merge `end2end-v2`)
- Parent-2: `925f4b4` (`feat/loki`)

---

## 1. Scope

Validate whether `feat/loki` merge introduces blocking issues for:
- distributed observability stack (Loki/Promtail/Prometheus/Grafana),
- distributed runtime compatibility,
- local simulation fallback path,
- env safety (restore to production Tailscale values).

---

## 2. Merge Delta Summary (P1 -> Merge)

Command basis:
- `git diff --name-status f0a79ba^1..f0a79ba`
- `git diff --stat f0a79ba^1..f0a79ba`

Changed files: 15 files, `+702/-4`

Main additions:
- `infra/loki/docker-compose.distributed.yml`
- `infra/loki/promtail-config.distributed.yml`
- `infra/prometheus/docker-compose.distributed.yml`
- `infra/prometheus/prometheus.distributed.yml`
- `infra/grafana/docker-compose.distributed.yml`
- `infra/grafana/provisioning/datasources/loki.distributed.yml`
- `infra/grafana/provisioning/datasources/clickhouse.distributed.yml`
- `health-check.sh`

---

## 3. Validation Plan

1. Static review of merge delta and new distributed observability files.
2. Compose parse checks for distributed monitoring stack.
3. Distributed-local simulation run (single host) to verify runtime behavior.
4. Validate ingest -> Kafka -> Flink -> ClickHouse path under distributed profile.
5. Restore production `.env` and clean up runtime state.
6. Record residual production-only checks.

---

## 4. Execution Log

### 4.1 Static review completed

- Confirmed distributed monitoring files and health-check script were added.
- Confirmed distributed runtime compose stack still references expected env keys.

### 4.2 Compose parse checks completed

Executed:
- `docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env config`
- `docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env config`
- `docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env config`

Result:
- all parse checks returned `ok`.

### 4.3 Distributed-local simulation completed

Execution flow:
- switched to local distributed simulation env,
- started distributed profile stack in order:
  - Kafka -> ClickHouse -> Ingestor -> Nginx -> Flink,
- validated health + ingest + Flink checkpoints + ClickHouse row growth.

Observed local-only issue:
- with one-host simulation, `FLINK_IP=<host_ip>` caused TM->JM registration failure.
- local fix used for simulation: `FLINK_IP=flink-jobmanager`.

Result:
- local distributed simulation passed after that one-host override.

### 4.4 Cleanup and restore completed

Actions:
- stopped distributed simulation stack,
- restored production env:
  - `config/.env <- config/.env.prod.tailscale.20260220-170627.bak`
- verified checksum match between restored `.env` and backup.

Result:
- environment returned to production/Tailscale baseline,
- no project containers left running.

---

## 5. Current Status

- Merge-level validation objective: **completed for single-host simulation**.
- Local distributed profile validation: **passed**.
- Loki-only single-host smoke validation: **passed after fixes**.
- Production Tailscale multi-host runtime validation: **pending** (machines must be online).

---

## 6. Residual Production-Only Checks

These cannot be fully validated in one-host simulation:

1. Cross-host network reachability/firewall on Tailscale addresses.
2. Real `FLINK_IP` as actual JM host IP (not Docker DNS alias).
3. Per-host Promtail shipping to central Loki under real host distribution.

---

## 7. Known Non-Blocking Gaps to Recheck

1. `health-check.sh` probes Kafka with HTTP on broker ports (`9092/9094/9096`), which is not a Kafka protocol health check.
2. Prometheus distributed targets expect exporters/endpoints that may not exist by default:
   - Kafka `:9308` exporter,
   - ClickHouse `:9363` metrics endpoint,
   - Nginx scrapeability as configured.
3. `promtail` fallback is now `http://loki:3100` for local simulation.
   - On real multi-host deployment, set `LOKI_URL` explicitly per machine to central Loki.

These are not merge blockers for runtime startup, but they can affect monitoring completeness.

---

## 8. Documentation Progress Note

- This file was created in `docs/current` to follow the same "plan + execution log" tracking style used during runtime stabilization.
- It is now the active checkpoint record for Loki merge validation under the current branch state.

---

## 9. Single-Host Loki Recheck (2026-02-20, focused)

Objective:
- verify only the merged Loki stack on one machine (defer full distributed/Tailscale test).

Execution:
1. Created required network by starting one Kafka service:
   - `docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env.distributed up -d kafka-1`
2. Started Loki stack:
   - `docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env.distributed up -d`
3. Checked status and health:
   - `docker compose ... ps -a` showed both `loki` and `promtail` exited with code 1.
   - `curl http://localhost:3100/ready` failed (connection refused).
4. Collected logs and cleaned up:
   - `docker logs loki`, `docker logs promtail`
   - `docker compose ... down` (Loki stack), `docker compose ... stop kafka-1`

Findings (blocking for Loki stack startup):
1. `infra/loki/docker-compose.distributed.yml:11`
   - bind mount `- /tmp/loki:/tmp/loki` caused:
   - `failed to create object client: mkdir /tmp/loki/chunks: permission denied`
2. `infra/loki/docker-compose.distributed.yml:33`
   - `LOKI_URL=$${LOKI_URL:-...}` is set but not exported before `envsubst`.
   - `promtail` startup error:
   - `failed to create client manager: at least one client config must be provided`

Recommended fixes:
1. Replace `/tmp/loki` bind mount with a named Docker volume (or ensure writable permissions for Loki runtime UID/GID).
2. Change command block to export `LOKI_URL` before `envsubst` (or inject `LOKI_URL` through compose `environment:`).

---

## 10. Immediate Solve Checklist

Items that must be fixed before Loki merge is considered runtime-safe:

1. Loki storage write permission
   - File: `infra/loki/docker-compose.distributed.yml:11`
   - Problem: `/tmp/loki` bind mount is not writable by Loki runtime user.
   - Done criteria: `loki` stays `Up` and `GET /ready` returns `200`.
   - Status: done.

2. Promtail client URL injection
   - File: `infra/loki/docker-compose.distributed.yml:33`
   - Problem: `LOKI_URL` is set in shell but not exported for `envsubst`.
   - Done criteria: `LOKI_URL` is injected at runtime and `promtail` stays `Up`.
   - Status: done.

3. Single-host smoke rerun after fixes
   - Scope: `kafka-1` + Loki stack only.
   - Done criteria:
     - `docker compose ... ps` shows `loki` and `promtail` running.
     - `curl http://localhost:3100/ready` succeeds.
     - `docker logs promtail` has no client-config startup error.
   - Status: done (readiness verified from inside Loki container).

---

## 11. Fix Implementation & Revalidation (2026-02-20)

Implemented changes:
1. `infra/loki/loki-config.yml`
   - `common.path_prefix`: `/tmp/loki` -> `/loki`
   - `storage_config.filesystem.directory`: `/tmp/loki/chunks` -> `/loki/chunks`
2. `infra/loki/docker-compose.distributed.yml`
   - Loki storage mount: host bind `/tmp/loki:/tmp/loki` -> named volume `loki-data:/loki`
   - Promtail runtime config path:
     - mount template as `/etc/promtail/config.yml` (read-only),
     - run with `-config.expand-env=true`,
     - inject `LOKI_URL` via compose `environment` (`${LOKI_URL:-http://loki:3100}`).
3. Version compatibility fix discovered during rerun:
   - `grafana/loki:2.9.0` -> `grafana/loki:3.3.2`
   - `grafana/promtail:2.9.0` -> `grafana/promtail:3.3.2`
   - Reason: Promtail 2.9 Docker discovery client was too old for current Docker API.

Revalidation evidence:
1. Single-host smoke rerun sequence executed:
   - `kafka-1` up -> Loki stack up -> status/log/ready checks -> cleanup.
2. Runtime status:
   - `docker compose ... ps -a` showed both `loki` and `promtail` as `Up`.
3. Loki readiness:
   - `GET /ready` from inside Loki container transitioned from `503` to `200` (`ready`).
4. Promtail health:
   - no `at least one client config must be provided` error.
   - no Docker API version mismatch error after image upgrade.
   - logs showed Docker targets being discovered.
5. Cleanup:
   - Loki stack down, `kafka-1` stopped, no project containers left running.
