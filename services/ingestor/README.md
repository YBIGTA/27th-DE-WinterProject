## Prerequisites

1. **Docker & Docker Compose**
2. **Kafka Running** (via Docker)
   ```bash
   docker compose -f ops/compose/single-machine/kafka.yml up -d
   ```

> All commands below assume you run them from the project root.

## Real Quick Start

**All services at once**  
docker compose -f ops/compose/single-machine/ingestor.yml up -d --build                                                                                                     
                                                                                                                                
**Individual — use these exact service names from ingestor.yml**

docker compose -f ops/compose/single-machine/ingestor.yml up -d ingestor-1                                                                                          
docker compose -f ops/compose/single-machine/ingestor.yml up -d ingestor-2                                                                                          
docker compose -f ops/compose/single-machine/ingestor.yml up -d ingestor-3                                                                                          
docker compose -f ops/compose/single-machine/ingestor.yml up -d nginx-lb

## Quick Start (Docker Cluster)

### 1. Start Ingestor Cluster (3 instances + Nginx LB)

```bash
docker compose -f ops/compose/single-machine/ingestor.yml up -d --build
```

This starts:

- `ingestor-1`, `ingestor-2`, `ingestor-3` (port 8081-8083)
- `nginx-lb` (port 8080) - load balancer

### 2. Check Status

```bash
docker compose -f ops/compose/single-machine/ingestor.yml ps
```

### 3. Run the Generator

```bash
cd services/generator
./build/generate
```

### 4. Monitor Logs

```bash
docker compose -f ops/compose/single-machine/ingestor.yml logs -f ingestor-1 ingestor-2 ingestor-3
```

### 5. Stop Cluster

```bash
docker compose -f ops/compose/single-machine/ingestor.yml down
```

## Local Development (Without Docker)

Requires Java 17+ and Gradle.

### Build & Run

```bash
./gradlew clean build
./gradlew bootRun
```

**Expected startup log:**

```text
[STARTUP] Initializing ingestion pipeline: buffer=10000, batch=500, timeout=50ms, topic=taxi-event-data
```

## Testing Scenarios

### Test 1: Normal Operation

**Goal**: Verify events flow end-to-end

1. Start ingestor: `./gradlew bootRun`
2. Start generator: `cd services/generator && ./build/generate`
3. Watch ingestor logs for metrics every 10 seconds:
   ```
   [METRICS] events_received=5000, events_processed=4998, events_failed=0,
             events_dropped=0, batches_sent=10, buffer_usage=5%, success_rate=99.96%
   ```

**Success criteria:**
- ✅ `events_received` increases steadily
- ✅ `events_processed` ≈ `events_received`
- ✅ `events_dropped` = 0
- ✅ `success_rate` > 99%
