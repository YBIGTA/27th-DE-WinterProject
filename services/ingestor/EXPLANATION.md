---
component: Ingestor
status: CURRENT
last_reviewed: 2026-02-21
core_files:
  - services/ingestor/src/main/java/com/ingestion/IngestionApplication.java
  - services/ingestor/src/main/java/com/ingestion/config/IngestorTuningProperties.java
  - services/ingestor/src/main/java/com/ingestion/config/ReactorKafkaConfig.java
  - services/ingestor/src/main/java/com/ingestion/controller/IngestionController.java
  - services/ingestor/src/main/java/com/ingestion/service/IngestionService.java
  - services/ingestor/src/main/java/com/ingestion/service/DeadLetterQueue.java
  - services/ingestor/src/main/java/com/ingestion/dto/TaxiEvent.java
  - services/ingestor/src/main/java/com/ingestion/dto/BatchIngestResponse.java
  - services/ingestor/src/main/resources/application.yml
  - services/ingestor/docker-compose.yml
  - services/ingestor/docker-compose.distributed.yml
  - infra/nginx/nginx.single-machine.conf
---

# Ingestor

## Role
An ingestion gateway that receives TaxiEvents from the Generator over HTTP, buffers them in an internal Reactive Sink, and forwards them asynchronously to the Kafka topic `taxi-event-data`.

## I/O Flow
```
[Generator (C++)] --(HTTP POST /ingest, JSON)--> [Nginx LB] --(random two least_conn)--> [Ingestor x3] --(Reactor Kafka, key=tripId)--> [Kafka: taxi-event-data]
```

## Implementation Logic

### Data Flow
```mermaid
flowchart TD
    G[Generator] -->|POST /ingest or /ingest/batch| N[Nginx random two least_conn LB]
    N -->|route| I1[Ingestor-1]
    N -->|route| I2[Ingestor-2]
    N -->|route| I3[Ingestor-3]

    subgraph "Inside each Ingestor"
        direction TB
        C[IngestionController] -->|tryEmitNext with bounded retry| S["Sinks.Many buffer (app.tuning.buffer.size)"]
        S -->|asFlux| BT["bufferTimeout(app.tuning.batch.size, app.tuning.batch.timeoutMs)"]
        BT -->|flatMap concurrency=app.tuning.kafka.sendConcurrency| K[KafkaSender.send]
        K -->|retryWhen(backoff x3) on stream error| KF[Kafka Broker]
        BT -->|serialization failure| DLQ[(DLQ file)]
    end
```

### Concurrency Model
- **Thread Model:** Event-loop (Netty). Both WebFlux and Reactor Kafka are fully non-blocking on the hot path. The app creates one explicit JVM shutdown-hook thread (`Runtime.addShutdownHook(new Thread(...))`) for graceful termination.
- **Shared State:**
  - `Sinks.Many<TaxiEvent>` — the single multicast buffer (capacity from `app.tuning.buffer.size`, code default 30,000; compose profiles currently override to 100,000). Multiple HTTP request threads can emit into it concurrently.
  - `AtomicLong` x5 (`eventsReceived`, `eventsProcessed`, `eventsFailed`, `eventsDropped`, `batchesSent`) — lock-free metric counters.
  - `KafkaSender` — singleton bean with a lazy connection; connects on the first `send()` call. Reactor-level `maxInFlight(1024)` caps the number of `SenderRecord`s in flight at the sender level; this is separate from the Kafka producer's `max.in.flight.requests.per.connection=5` which caps TCP-level requests per broker connection.
  - `DeadLetterQueue` — file-appending DLQ for events that fail serialization. Writes JSONL to `app.dlq.filepath` (default `dead_letter_queue.jsonl`). Thread-safe via `synchronized`.
- **Sync Primitives:**
  - In-service bounded retry loop for `FAIL_NON_SERIALIZED` — retries up to 10 times with exponential `LockSupport.parkNanos` backoff (50us -> 2ms cap).
  - `AtomicLong` — CAS-based lock-free increment for counters.
  - `AtomicInteger` — used in the batch endpoint to track `successCount` / `failedCount`.
  - `synchronized` — used in `DeadLetterQueue` for thread-safe file writes.

### Core Algorithm

**Single-event path (`POST /ingest`)**
1. WebFlux deserializes the request body into a `TaxiEvent`.
2. `IngestionService.ingest()` is called, which attempts `sink.tryEmitNext(event)`.
3. If the result is `FAIL_NON_SERIALIZED`, `IngestionService` retries `tryEmitNext` up to 10 times with bounded exponential nano-sleep backoff.
4. If the result is `FAIL_OVERFLOW`, the event is dropped and HTTP 429 is returned.
5. If `OK`, HTTP 202 is returned immediately. The actual Kafka send happens asynchronously in the pipeline below.

**Batch path (`POST /ingest/batch`)**
1. The event list is iterated sequentially; each event is passed to `ingest()`.
2. On `FAIL_OVERFLOW`, the loop exits immediately. Unprocessed events are counted as rejected, and HTTP 429 is returned with a `BatchIngestResponse` body.
3. Partial success returns HTTP 207; full success returns HTTP 202.

**Internal pipeline (wired in `IngestionService @PostConstruct`)**
1. `sink.asFlux()` — consumes events from the buffer as a Flux.
2. `bufferTimeout(batch.size, batch.timeoutMs)` — forms a batch when either configured size accumulates or configured timeout elapses (`app.tuning.batch.*`, code defaults: 500 / 10 ms).
3. `flatMap(sendBatchToKafkaReactive, sendConcurrency)` — configured concurrent batch sends (`app.tuning.kafka.sendConcurrency`, default: 4).
4. Within each batch, a `SenderRecord` is created with `tripId` as the key and the Jackson-serialized JSON as the value.
5. `kafkaSender.send()` is called. `SenderResult.exception()` failures are counted and logged per record; retry is **not** triggered by that signal alone.
6. `Retry.backoff(3, 100ms, maxBackoff=2s)` handles stream-level errors from `kafkaSender.send()` (batch-level retry).
7. If the entire subscription terminates, `.retry()` re-subscribes automatically.

**Metrics & Shutdown**
- `Flux.interval(metrics.intervalSec)` — logs received / processed / failed / dropped / batch / DLQ-written counts on configured interval (`app.tuning.metrics.intervalSec`, default 10 seconds).
- `@PreDestroy` — calls `sink.tryEmitComplete()`, waits 5 s to let the pipeline drain, closes the `KafkaSender`, then closes the `DeadLetterQueue` file.
- `Runtime.addShutdownHook` — closes the Spring application context.

## Data Contract
- **Input:**
  - `POST /ingest` — body: a single `TaxiEvent` JSON object (see schema below).
  - `POST /ingest/batch` — body: a JSON array of `TaxiEvent` objects. An empty or `null` array returns HTTP 400.
  - `GET /health` — no parameters.
- **Output:**
  - Kafka topic `taxi-event-data` — key: `tripId` (String), value: `TaxiEvent` JSON. Partition assignment is determined by key hashing.
  - Dead letter queue file (`app.dlq.filepath`, default `dead_letter_queue.jsonl`) — JSONL format, one line per failed event with fields: `timestamp`, `tripId`, `eventData`, `errorClass`, `errorMessage`, `stackTrace`.
  - HTTP responses: 202 (accepted), 429 (buffer full), 207 (partial batch success), 400 (empty batch), 500 (other failures).
  - `/ingest/batch` response body: `{ acceptedCount, rejectedCount, failedIndices[] }`.
    - **Note:** On overflow mid-batch, `rejectedCount` includes both the failed event and all *unprocessed* events after it, but `failedIndices` only contains indices that were actually attempted. The unprocessed tail is not listed in `failedIndices`.
- **TaxiEvent Schema:**
  Three event types share a single DTO. Fields not applicable to an event type are serialized as `null`.

  | Field (JSON key) | Type | Applicable to |
  |------------------|------|---------------|
  | `event` | String | ALL — `PICKUP`, `IN_TRANSIT`, or `DROPOFF` |
  | `trip_id` | Long | ALL — used as Kafka message key |
  | `ts` | String | ALL — ISO 8601 timestamp |
  | `lat` | Double | ALL — current latitude |
  | `lon` | Double | ALL — current longitude |
  | `PULocationID` | Long | PICKUP, DROPOFF |
  | `DOLocationID` | Long | PICKUP, DROPOFF |
  | `VendorID` | Long | PICKUP, DROPOFF |
  | `passenger_count` | Long | PICKUP, DROPOFF |
  | `RatecodeID` | Long | PICKUP, DROPOFF |
  | `payment_type` | Long | PICKUP, DROPOFF |
  | `extra` | Double | PICKUP, DROPOFF |
  | `mta_tax` | Double | PICKUP, DROPOFF |
  | `tip_amount` | Double | PICKUP, DROPOFF |
  | `tolls_amount` | Double | PICKUP, DROPOFF |
  | `improvement_surcharge` | Double | PICKUP, DROPOFF |
  | `congestion_surcharge` | Double | PICKUP, DROPOFF |
  | `Airport_fee` | Double | PICKUP, DROPOFF |
  | `fare_amount` | Double | DROPOFF |
  | `total_amount` | Double | DROPOFF |
  | `trip_distance` | Double | DROPOFF |
- **Invariants:**
  - `tripId` is expected from the upstream producer and is used as the Kafka message key. The ingestor does not validate non-null; if absent, the key becomes `"null"` (string), and those events hash to the same partition.
  - When the sink buffer exceeds configured capacity (`app.tuning.buffer.size`), new events are dropped and the client receives 429. No internal retry for overflow.
  - `acks=1` (leader ack only) — only the Kafka leader's receipt is confirmed. Follower replication failures are not detected.

## Design Decisions
| Decision | Why | Trade-off |
|----------|-----|-----------|
| Spring WebFlux (Netty event-loop) | Handles HTTP reception in a non-blocking manner, achieving high concurrency without being limited by thread count | Higher debugging complexity compared to Spring MVC |
| Reactor Kafka (`KafkaSender`) | Kafka produce is also non-blocking, so it never blocks the HTTP event-loop | More complex API compared to Spring Kafka's `KafkaTemplate` |
| `Sinks.Many` multicast buffer (code default 30,000, tunable) | Decouples HTTP receive speed from Kafka send speed, absorbing latency spikes | Events can be lost on buffer overflow |
| `bufferTimeout` (code default 500 events / 10ms, tunable) | Dramatically reduces Kafka network round-trips compared to sending events individually | Introduces batching latency up to the configured timeout |
| `acks=1` (leader only) | Maximizes throughput | Acknowledged messages can be lost if the leader fails before replication |
| LZ4 compression | Reduces network bandwidth with minimal CPU overhead | Lower compression ratio than Snappy or Gzip |
| Nginx `random two least_conn` | Balances fairness and lower scheduling overhead under high concurrency | Adds randomness, so assignment is less globally optimal than full least-conn |
| 3-instance cluster | Horizontally scales both buffer capacity and Kafka send throughput beyond a single instance | Each instance holds an independent Sink, so event ordering by `tripId` is not guaranteed at the cluster level |
| `stopOnError(false)` on KafkaSender | A single event's serialization or send failure does not terminate the entire pipeline | Failed events are written to the DLQ instead of being silently skipped |
| File-based DLQ for serialization failures | Events that fail JSON serialization are persisted to a JSONL file for inspection and replay, rather than being silently discarded | Uses `event.toString()` (Lombok-generated) since `ObjectMapper` is the thing that failed; manual JSON build avoids dependency on the failing serializer |
| Kafka producer config in `ReactorKafkaConfig` (not `application.yml`) | `KafkaSender` is built programmatically; `spring.kafka.producer.*` in `application.yml` is **not** read by the Reactor Kafka sender | Editing producer settings in `application.yml` has no effect — changes must be made in `ReactorKafkaConfig.java` |
| Runtime tuning via `app.tuning.*` | Batch/buffer/concurrency/metrics knobs can be changed via env vars (`APP_TUNING_*`) without code changes | Deployed values can diverge by environment if not documented |

## Failure Modes & Handling
| Failure | Detection | Response |
|---------|-----------|----------|
| Sink buffer overflow (exceeds configured capacity) | `tryEmitNext()` returns `FAIL_OVERFLOW` | Event is dropped; HTTP 429 is returned. The Generator backs off via its own rate limiter and retries. |
| Concurrent emit collision | `tryEmitNext()` returns `FAIL_NON_SERIALIZED` | `IngestionService` retries boundedly (max 10). If still not `OK`, controller returns `503` for that event. |
| Kafka record send failure (broker/network/timeout) | `SenderResult.exception() != null` | Failed record is logged and `eventsFailed` increments. Other records in the stream continue (`stopOnError(false)`). |
| Kafka sender stream error | Error signal from `kafkaSender.send(records)` Flux | `Retry.backoff(3, 100ms, max 2s)` retries the batch send; if still failing, batch-level error is logged. |
| JSON serialization failure | Exception thrown by `ObjectMapper.writeValueAsString()` | The affected event is written to the file-based DLQ (`DeadLetterQueue`), `eventsFailed` increments, and the event is skipped for Kafka (`Mono.empty()`); the rest of the batch continues. |
| Pipeline subscription termination | Terminal `onError` signal in the Flux chain | `.retry()` automatically re-subscribes to the pipeline. |
| Ingestor instance crash / upstream failure | Upstream connection errors / 502/503/504 at Nginx; container healthcheck (`GET /health`) also marks unhealthy at compose level (10 s interval, 3 retries) | Nginx `random two least_conn` + request-level retry (`proxy_next_upstream`, max 2 tries) routes traffic to remaining instances. |
| Graceful shutdown | JVM shutdown hook + Spring `@PreDestroy` | Sink is completed, pipeline drains for up to 5 s, then `KafkaSender` and `DeadLetterQueue` are closed. |
| Kafka broker unavailable at startup | `KafkaSender` uses a lazy connection | The application starts without blocking. Actual send failures are covered by the retry policy. |
