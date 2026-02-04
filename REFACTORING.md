# Refactoring Plan (Ops-Centric Layout)

This document captures the intended refactor from the current layout to an ops-centric layout, including the target structure, a safe move plan, and the files that will require path updates.

## Goals
1. Separate runtime concerns from application code.
2. Make deployment and operational procedures discoverable in one place.
3. Preserve current behavior while only changing paths and references.
4. Defer config restructuring until after the directory refactor is stable.

## Phased Approach
### Phase 1: Structure-Only Refactor (Now)
Focus on moving directories and updating paths without changing configuration semantics.
1. Move modules and compose files into the new `services/`, `infra/`, and `ops/` layout.
2. Update docs and scripts to point to new locations.
3. Keep existing env usage and config files as-is to avoid behavior changes.

### Phase 2: Config Consolidation (Next)
Introduce a clearer config model after structure is stable.
1. Define per-component config locations and conventions.
2. Decide which values stay in env vs. component config files.
3. Update compose and docs to use the new config conventions.

## Target Structure
```text
.
├── services
│   ├── ingestor
│   ├── generator
│   ├── preprocess
│   └── flink-job
├── infra
│   ├── kafka
│   ├── clickhouse
│   ├── flink
│   ├── spark
│   └── connectors
├── ops
│   ├── compose
│   │   ├── single-machine
│   │   └── distributed
│   └── scripts
├── config
│   ├── .env.single-machine
│   ├── .env.distributed
│   └── README.md
├── data
├── analysis
└── README.md
```

## Move Map
| Current Path | New Path |
| --- | --- |
| `ingestor/` | `services/ingestor/` |
| `generator/` | `services/generator/` |
| `preprocess/` | `services/preprocess/` |
| `jobs/flink-job/` | `services/flink-job/` |
| `connectors/` | `infra/connectors/` |
| `infra/kafka/docker-compose*.yml` | `ops/compose/single-machine/` or `ops/compose/distributed/` |
| `infra/clickhouse/docker-compose.yml` | `ops/compose/single-machine/` and `ops/compose/distributed/` |
| `infra/flink/docker-compose*.yml` | `ops/compose/single-machine/` or `ops/compose/distributed/` |
| `ingestor/docker-compose*.yml` | `ops/compose/single-machine/` or `ops/compose/distributed/` |
| `ingestor/nginx*.conf` | `ops/compose/distributed/` |

Notes on compose placement:
1. Place single-machine compose files under `ops/compose/single-machine/`.
2. Place multi-machine or per-node compose files under `ops/compose/distributed/`.
3. Keep `infra/` for vendor or cluster-specific definitions that are not “how to run” entrypoints.

Decision notes:
1. `single-machine` exists for local end-to-end pipeline testing on one instance.
2. `distributed` exists for multi-instance deployment and per-node definitions.
3. Duplicate compose content between the two is acceptable given the different goals.

## Migration Steps
1. Create new directories: `services/`, `ops/compose/single-machine/`, `ops/compose/distributed/`, `ops/scripts/`.
2. Move application modules into `services/` based on the Move Map.
3. Move compose and ops files into `ops/compose/...` based on the Move Map.
4. Update all path references in docs and scripts.
5. Run compose from within `ops/compose/...` directories to preserve relative paths.
6. If running from elsewhere, pass `--project-directory` and verify relative paths.
7. Run a dry start for each mode (single-machine and distributed).

## Files Likely Needing Updates
1. `config/README.md` for compose file paths.
2. `infra/flink/README.md` for paths to compose or resources.
3. Root `README.md` for the directory tree.
4. Any scripts or runbooks that reference old paths.
5. Any compose `volumes:` or `build:` paths using relative references.

## Non-Goals
1. No changes to service logic or configuration values.
2. No changes to Docker images or dependency versions.

## Validation Checklist
1. `config/README.md` paths match new locations.
2. `docker compose -f ... up -d` works for both modes.
3. Flink job build and copy steps still point to the correct paths.
4. Nginx upstream configuration uses correct IPs for distributed mode.
