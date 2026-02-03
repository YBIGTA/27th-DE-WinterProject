# config

Environment templates and deployment scripts for the data pipeline.

All scripts are run from the **project root**, not from this folder.

---

## Quick Start

### Single machine (local dev)

```bash
./config/start-single-machine.sh
```

This does everything: picks the right env, starts Kafka, then starts the ingestor cluster. After it's up, run the generator:

```bash
cd generator && ./build/generate
```

### Distributed (5 machines)

```bash
# 1. Edit the IPs to match your network
vim config/.env.distributed

# 2. Also update upstream IPs in nginx config (doesn't support env vars)
vim ingestor/nginx.distributed.conf

# 3. Activate and copy to all machines
./config/use-env.sh distributed
# scp .env to each of the 5 machines

# 4. Follow the per-machine steps
./config/deploy-distributed.sh
```

---

## What's in this folder

| File | Purpose |
|------|---------|
| `.env.single-machine` | Template for single-machine deployment (Version 1) |
| `.env.distributed` | Template for 5-machine deployment (Version 2) |
| `.env.example` | Minimal reference showing all available variables |
| `use-env.sh` | Copies a template to the project root as the active `.env` |
| `start-single-machine.sh` | One-command local deployment (Kafka + ingestors) |
| `deploy-distributed.sh` | Step-by-step guide for distributed deployment |
| `EXPLANATION.md` | Full design doc: data flow, decisions, failure modes |

---

## How it works

Templates live here. The active config lives at the **project root as `.env`**.

```
config/.env.single-machine  ──┐
                               ├── use-env.sh ──> /.env  <── Docker + Generator read this
config/.env.distributed     ──┘
```

`use-env.sh` is the only thing that writes to `/.env`. Everything else (Docker Compose, the generator) reads from there. Switching deployments is just switching which template gets copied.

---

## Distributed: IPs to update

Two places need manual IP edits before deploying distributed:

1. `config/.env.distributed` -- the `*_IP` variables at the top, plus `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_ADVERTISED_LISTENERS`, and the `NGINX_UPSTREAM_*` entries
2. `ingestor/nginx.distributed.conf` -- the `upstream` block (Nginx doesn't support env var substitution)
