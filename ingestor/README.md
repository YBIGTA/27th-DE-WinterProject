## Prerequisites

1. **Java 17+**
   ```bash
   java --version
   ```
   I recommend JDK 21

2. **Kafka Running** (via Docker)
   ```bash
   cd ../infra/kafka
   docker-compose up -d
   ```

3. **Generator Built**
   You can skip if you are just trying to see if the pipeline works
   But when you run this generator you might encounter many issue ^^

   ```bash
   cd ../generator
   # Build if not already done
   cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release

   cmake --build build
   ```

## Quick Start

### 1. Build the Ingestor

```bash
cd ingestor
./gradlew clean build
```

Expected output: `BUILD SUCCESSFUL`

OR TRY THIS

gradle clean build

### 2. Start the Ingestor

```bash
./gradlew bootRun
```
OR TRY

gradle bootRun

**Expected startup log:**
```
[STARTUP] Initializing ingestion pipeline: buffer=10000, batch=500, timeout=50ms, topic=taxi-event-data
```

### 3. Run the Generator

```bash
cd ../generator
./build/generate
```

The generator will start sending taxi events to the ingestor.

## Testing Scenarios

### Test 1: Normal Operation

**Goal**: Verify events flow end-to-end

1. Start ingestor: `./gradlew bootRun`
2. Start generator: `cd ../generator && ./build/generate`
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

