---
component: Environment-Based Deployment Configuration System
status: CURRENT
last_reviewed: 2026-02-03
core_files:
  - config/.env.single-machine
  - config/.env.distributed
  - config/.env.example
  - config/use-env.sh
  - config/start-single-machine.sh
  - config/deploy-distributed.sh
  - generator/generate.cpp (load_env_file)
  - ingestor/docker-compose.yml
  - ingestor/docker-compose.ingestor-{1,2,3}.yml
  - ingestor/docker-compose.nginx.yml
  - ingestor/nginx.distributed.conf
  - infra/kafka/docker-compose.yml
---

# Environment-Based Deployment Configuration System

## Role
Enable two fundamentally different deployment topologies (single-machine vs 5-machine distributed) from a single codebase using file-based environment variable templates.

## I/O Flow
```
[Template Selection] --(File Copy)--> [Active .env] --(Parse)--> [Runtime Components]
                                                                  ├─ Generator (C++ setenv)
                                                                  ├─ Ingestors (Docker env_file)
                                                                  ├─ Kafka (Docker env_file)
                                                                  └─ Nginx (Docker env_file)
```

## Implementation Logic

### Data Flow
```mermaid
flowchart TD
    A[User runs use-env.sh] --> B{Version?}
    B -->|single-machine| C[Copy .env.single-machine → .env]
    B -->|distributed| D[Copy .env.distributed → .env]

    C --> E[.env file on disk]
    D --> E

    E --> F[Generator reads ../.env]
    E --> G[Docker Compose reads env_file]

    F --> H[load_env_file parses]
    H --> I[setenv injects to process]
    I --> J[getenv INGEST_URL at runtime]

    G --> K[Docker substitutes ${VAR}]
    K --> L[Container env vars]
    L --> M[Spring Boot reads SPRING_KAFKA_BOOTSTRAP_SERVERS]
```

### Concurrency Model
- **Thread Model:** N/A (Configuration loading only)
- **Shared State:** File system (.env file) - read-only after copy
- **Sync Primitives:** None required
- **Mutability:** Immutable after process startup
  - Generator: Loads once at main() entry via load_env_file()
  - Docker: Reads once at container creation
  - Changes require restart

### Core Algorithm

#### 1. Template Switching (use-env.sh)
```bash
SOURCE_FILE=".env.${VERSION}"  # .env.single-machine OR .env.distributed
TARGET_FILE=".env"
cp "$SOURCE_FILE" "$TARGET_FILE"
```
**Atomicity:** Uses cp (atomic overwrite on Linux)

#### 2. Generator Parsing (generate.cpp:294-321)
```cpp
load_env_file("../.env");  // Relative to generator/build/
  ↓ For each line:
  ↓   Skip if empty or starts with '#'
  ↓   Parse KEY=VALUE
  ↓   Strip quotes from VALUE
  ↓   setenv(KEY, VALUE, 0)  // 0 = don't overwrite existing
```
**Priority:** Shell env > .env file > Application defaults

Key usage:
- `INGEST_URL` → HTTP endpoint for sending events (generate.cpp:331, 392)

#### 3. Docker Env File Loading
```yaml
services:
  ingestor-1:
    env_file: ../.env
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: ${SPRING_KAFKA_BOOTSTRAP_SERVERS}
```
**Mechanism:**
1. Docker reads `../.env` before container start
2. Substitutes `${VAR}` placeholders in `environment:` section
3. Passes final values as container environment variables

Key variables consumed:
- `SPRING_KAFKA_BOOTSTRAP_SERVERS` → Kafka broker address (Ingestors)
- `KAFKA_ADVERTISED_LISTENERS` → External/internal listeners (Kafka)
- `NGINX_LB_PORT`, `INGESTOR_*_PORT` → Port mappings

## Data Contract

### Input Format (.env files)
```bash
# Comments allowed
KEY=VALUE
KEY_WITH_QUOTES="value with spaces"  # Quotes stripped by generator parser
KEY_NUMERIC=8080
KEY_LIST=server1:9092,server2:9092  # Application parses
```

### Configuration Variables

| Variable | Type | Version 1 | Version 2 | Consumer |
|----------|------|-----------|-----------|----------|
| `INGEST_URL` | URL | `http://localhost:8080/ingest` | `http://localhost:8080/ingest` | Generator |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Hostport | `kafka:29092` | `192.168.1.50:9092` | Ingestors |
| `KAFKA_ADVERTISED_LISTENERS` | List | `...localhost:9092...` | `...192.168.1.50:9092...` | Kafka |
| `NGINX_UPSTREAM_*` | Hostport | `ingestor-*:8080` | `192.168.1.*:8080` | Nginx (manual edit) |

### Invariants
1. **Network Reachability:**
   - V1: All services on `localhost` or Docker network names
   - V2: All IPs must be on same L2/L3 network segment
2. **Port Uniqueness:**
   - V1: External ports (8080-8083, 9092, 8090) must not conflict
   - V2: Same port (8080) can be reused across machines
3. **Kafka Listener Consistency:**
   - `KAFKA_ADVERTISED_LISTENERS` must match client-facing network
   - `INTERNAL` listener for Docker network, `EXTERNAL` for host network

## Design Decisions

| Decision | Why | Trade-off |
|----------|-----|-----------|
| **File-based templates** (vs centralized config server) | Zero dependencies, works offline, version controlled | Manual propagation to 5 machines in V2 |
| **Explicit copy** (vs symlinks) | Works across OS (Windows/Linux), no dangling symlinks | Must remember to run `use-env.sh` |
| **Separate compose files** for V2 (vs overrides) | Clear separation, no conditional logic, deploy per-machine | More files to maintain (5 docker-compose files) |
| **C++ native parser** (vs libdotenv) | No external dependency, 30 LOC | Custom implementation, no advanced features (variable expansion) |
| **env_file + environment** (vs only env_file) | Explicit variable mapping, validation via Docker | Redundancy, must list variables twice |
| **Hardcoded IPs in nginx.distributed.conf** | Nginx doesn't support env var substitution in upstream | Manual edit required before V2 deployment |

## Deployment Topologies

### Version 1: Single Machine
```
┌─────────────────────────────────────────┐
│  Host Machine (localhost)               │
│  ┌─────────────────────────────────┐    │
│  │  Docker Network (kafka-network) │    │
│  │  ┌──────┐  ┌──────────────┐     │    │
│  │  │Kafka │  │Ingestor-1,2,3│     │    │
│  │  │:29092│  │:8080         │     │    │
│  │  └──────┘  └──────────────┘     │    │
│  │       ↑            ↑             │    │
│  │       │            │             │    │
│  │  ┌────┴────┐  ┌────┴─────┐      │    │
│  │  │Nginx:80 │  │kafka:9092│      │    │
│  │  └─────────┘  └──────────┘      │    │
│  └─────────────────────────────────┘    │
│         ↑                                │
│    Generator (native)                    │
│    → localhost:8080/ingest               │
└─────────────────────────────────────────┘
```

**Characteristics:**
- All networking via `localhost` or Docker DNS
- Ports exposed: 8080 (Nginx), 8081-8083 (direct ingestor), 9092 (Kafka), 8090 (UI)
- Single `docker compose up -d` deploys everything

### Version 2: Distributed (5 Machines)
```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ Machine A    │      │ Machine B    │      │ Machine C    │
│ (Generator)  │      │ (Ingestor-1) │      │ (Ingestor-2) │
│              │      │              │      │              │
│ Generator────┼──┐   │ Ingestor-1   │      │ Ingestor-2   │
│   ↓          │  │   │   :8080      │      │   :8080      │
│ Nginx:8080   │  │   └──────────────┘      └──────────────┘
└──────────────┘  │          ↓                      ↓
                  │   ┌──────────────┐      ┌──────────────┐
                  │   │ Machine D    │      │ Machine E    │
                  │   │ (Ingestor-3) │      │ (Kafka)      │
                  │   │              │      │              │
                  └──→│ Ingestor-3   │      │ Kafka:9092   │
                      │   :8080      │      │              │
                      └──────────────┘      └──────────────┘
                             ↓                      ↑
                             └──────────────────────┘
```

**Characteristics:**
- Cross-machine TCP communication (192.168.1.x)
- Each machine deploys ONE service
- Nginx load balances to 3 IPs
- Requires manual IP editing before deployment

## Failure Modes & Handling

| Failure | Detection | Response | Recovery |
|---------|-----------|----------|----------|
| **Missing .env file** | Generator: Silent fallback to defaults<br>Docker: Container fails to start | Generator uses hardcoded `http://localhost:8080`<br>Docker shows env var substitution error | Run `use-env.sh` before deployment |
| **Wrong IPs in V2** | Connection timeout (Generator→Nginx, Ingestor→Kafka) | HTTP 503 / Kafka connection errors in logs | Edit `.env.distributed` + `nginx.distributed.conf`, redeploy |
| **Port conflict in V1** | Docker bind error: "port already in use" | Container fails to start | Change `*_PORT` variables in `.env.single-machine` |
| **Stale .env after switching** | Services connect to wrong endpoints | Data goes to old Kafka, events lost | Always run `use-env.sh` before starting services |
| **Forgot to copy .env to all machines (V2)** | Each machine reads different config | Ingestors point to wrong Kafka, inconsistent state | `scp .env` to all 5 machines |
| **Nginx can't resolve DNS (V2)** | `nginx: [emerg] host not found in upstream` | Nginx container fails to start | Use IPs not hostnames in `nginx.distributed.conf` |

## Operational Procedures

### Switching Environments
```bash
# Development on single machine
./config/use-env.sh single-machine
./config/start-single-machine.sh

# Production distributed setup
./config/use-env.sh distributed
# Edit config/.env.distributed and nginx.distributed.conf first!
# Then deploy per-machine (see config/deploy-distributed.sh)
```

### Verifying Active Configuration
```bash
# Check which template is active
head -1 .env  # Shows "VERSION 1" or "VERSION 2" comment

# Verify Generator will use correct URL
grep INGEST_URL .env

# Verify Ingestors will connect to correct Kafka
grep SPRING_KAFKA_BOOTSTRAP_SERVERS .env

# Test config without deploying
docker compose -f ingestor/docker-compose.yml config | grep SPRING_KAFKA
```

### Debugging Configuration Issues
```bash
# Generator: Check what env vars it loaded
./generator/build/generate  # Logs show config loading

# Ingestor: Check env vars inside container
docker exec ingestor-1 env | grep KAFKA

# Kafka: Verify advertised listeners
docker logs kafka | grep ADVERTISED

# Nginx: Check upstream resolution
docker exec ingestor-lb nginx -T | grep upstream
```

## Security Considerations
1. **No secrets in .env files** - These are version controlled
   - If needed: Use `.env.local` (gitignored) for secrets, source after `.env`
2. **Network exposure:**
   - V1: Only localhost ports exposed
   - V2: All services exposed on 0.0.0.0 - use firewall rules
3. **File permissions:** `.env` is world-readable (needed by Docker)
   - Sensitive configs should use Docker secrets or external secret managers

## Future Improvements
1. **Variable validation:** Pre-flight check script to verify IPs are reachable
2. **Dynamic Nginx config:** Generate `nginx.distributed.conf` from `.env` variables
3. **Deployment automation:** Ansible/Terraform for V2 multi-machine setup
4. **Health checks:** Verify end-to-end connectivity before declaring deployment successful
5. **Rollback mechanism:** Keep previous `.env` as `.env.backup`
