---
component: Flink Processor (TaxiRealtimeJob)
status: CURRENT
last_reviewed: 2026-02-21
core_files:
  - services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java
  - services/flink-job/src/main/java/com/example/TaxiEvent.java
  - services/flink-job/src/main/java/com/example/SpatialJoinFunction.java
  - services/flink-job/pom.xml
  - infra/clickhouse/schema.sql
  - infra/flink/docker-compose.distributed.yml
  - infra/flink/docker-compose.yml
---

# Flink Processor

## Role
Kafka로부터 유입되는 원시 택시 이벤트를 수집하여 **순서 재정렬(Reordering)** 후 ClickHouse raw 테이블(`taxi_events`, `taxi_predictions`)에 적재하고, 조회는 dedup serving 뷰(`taxi_events_latest`, `taxi_predictions_latest`) 기준으로 수행하도록 설계된 스트리밍 컴포넌트입니다.

## I/O Flow
`[Kafka Source] --(TaxiEvent)--> [TaxiRealtimeJob] --(JDBC Sink)--> [ClickHouse Raw: taxi_events]
                                        L--(3m Demand -> ONNX)--> [ClickHouse Raw: taxi_predictions]
                                        L--(Materialized View / FINAL Read)--> [ClickHouse Serving: taxi_events_latest / taxi_predictions_latest]
                                        L--(Log Out)--> [Console: ML_DEMAND]`

## Implementation Logic

### Data Flow
```mermaid

flowchart TD

    A[Kafka Source] --> B{Safe Deserializer}
    B -- Valid --> C[Watermark Assigner]
    B -- Invalid/Null Payload --> D[DLQ: JSONL Write]
    C --> C1{Required Fields + ts Parse}
    C1 -- Invalid --> D1[DLQ: JSONL Write]
    C1 -- Valid --> E[KeyBy: trip_id]
    E --> F[Process: Per-Trip Reorder]
    F --> N[Side Output: LATE_EVENTS (관측용)]
    F --> G[Map: Spatial Join]
    G --> H[Filter: zone_id != null]
    H --> I[Track 1: JDBC Sink (taxi_events raw)]
    H --> J[Filter: PICKUP Event]
    J --> K[Window: 3min Tumbling]
    K --> L[Aggregate: Demand Count]
    L --> M[Track 2: Print Sink]
    L --> P[Track 3: KeyedProcess lag_20 state]
    P --> Q[ONNX Inference]
    Q --> R[JDBC Sink: taxi_predictions raw]
    I --> I2[MV: taxi_events_serving]
    R --> R2[MV: taxi_predictions_serving]
    I2 --> I3[View: taxi_events_latest FINAL]
    R2 --> R3[View: taxi_predictions_latest FINAL]
```

### Concurrency Model
* **Thread Model**: 병렬도는 `FLINK_PARALLELISM`(기본 12)와 TaskManager 슬롯 설정에 따라 배치됩니다.
* **Shared State**:
    * **bufferState (ListState)**: 각 `trip_id`별 재정렬을 위한 대기 큐입니다.
    * **lastEmittedTs (ValueState)**: 이미 배출된 데이터보다 과거 데이터가 들어오는 것을 방지하기 위한 타임스탬프 기록기입니다.
    * **lastSeenProcTime (ValueState)**: 키별 마지막 처리 시각(Processing Time)입니다.
    * **cleanupTimerProcTs (ValueState)**: 키별 idle cleanup 타이머 시각입니다.
    * **zone-demand-history (ListState<Long>)**: zone별 최근 demand 히스토리(`lag_20`) 상태입니다.
* **State Management**: **Idle Cleanup** 로직을 통해 20분간 활동이 없는 키의 상태를 자동 삭제하여 메모리 누수를 방지합니다.

### Core Algorithm
* **Per-Trip Reordering**: `KeyedProcessFunction` 내에서 `TimerService`를 활용합니다. Watermark가 데이터 시각 + 5초를 지나는 시점에 버퍼를 소팅하여 배출합니다.
* **3분 집계**: `DemandAggregator`를 통해 `3분 Tumbling` 윈도우 기준 zone별 PICKUP 수요를 집계합니다.
* **ONNX 추론**: zone별 히스토리에서 `lag_20`이 확보된 시점부터 피처(`zone_id`,`hour`,`day_of_week`,`is_weekend`,`demand_lag_20`)를 생성해 ONNX로 추론하고, `prediction_time`/`target_time(+15분)`과 함께 ClickHouse에 적재합니다.

## Data Contract
* **Input**: Kafka `taxi-event-data` 토픽의 JSON 데이터. 필수는 `trip_id`, `ts`이며 `lat`/`lon`/`event`는 nullable로 처리됩니다.
* **Output**:
    * **ClickHouse Raw**: `taxi_events`, `taxi_predictions` (JDBC sink 직접 적재 대상).
    * **ClickHouse Serving**: `taxi_events_latest`, `taxi_predictions_latest` (중복 완화 조회 대상).
    * **DLQ File (JSONL)**: Flink drop/실패 이벤트 기록 (`/opt/flink/data/dead_letter_queue-<hostname>.jsonl`, init 실패 시 `/tmp/dead_letter_queue-<hostname>.jsonl`).
    * **Console**: `[ML_DEMAND] Zone: {id}, Time: {ts}, Count: {n}`.
* **Invariants**:
    * `trip_id`/`ts` 누락 또는 `ts` 파싱 실패 데이터는 **raw filter 단계에서 DLQ(JSONL) 기록 후 drop**됩니다.
    * `lat`/`lon` 누락 이벤트는 drop되지 않고 `zone_id=-1`로 전달됩니다(`SpatialJoinFunction`).
    * `LATE_EVENTS` 사이드 아웃풋은 워터마크 지연(`eventTs <= wm`) 및 재정렬 중 역전(`ts < lastEmittedTs`) 이벤트를 전달합니다.
    * late 이벤트도 `LATE_DROPPED` 카테고리로 DLQ(JSONL)에 기록됩니다.
    * 예측은 `lag_20` 상태가 누적된 이후부터만 생성됩니다.
    * JDBC sink는 at-least-once 특성이므로 raw 테이블 중복 가능성이 있으며, 운영 조회는 `*_latest` 뷰 기준으로 수행합니다.

## Design Decisions
| Decision | Why | Trade-off |
| :--- | :--- | :--- |
| **ONNX 추론을 Job 내부에 내장** | Kafka→Flink→ClickHouse 체인 내에서 실시간 예측 저장까지 완료하기 위함입니다. | TaskManager 메모리/CPU 사용량이 증가합니다. |
| **모델 파일 bind mount 사용** | 이미지 재빌드 없이 모델 교체/검증이 가능하기 때문입니다. | 경로/파일 누락 시 잡 시작 시점 에러가 발생합니다. |
| **병렬도 env 기반 설정** | 환경별로 처리량/안정성을 빠르게 조정하기 위함입니다. | 슬롯 부족 시 태스크가 `scheduled`로 대기할 수 있습니다. |
| **Idle Cleanup (20m)** | 분산 환경에서 무한히 늘어날 수 있는 상태(State) 크기를 제어하기 위함입니다. | 20분 이상 지연된 매우 늦은 데이터는 정렬 혜택을 받지 못합니다. |
| **JDBC Batch (기본 50000)** | 코드 기본값은 `50000/3000ms`이며 compose 프로파일에 따라 오버라이드해 적재 성능을 튜닝합니다. | single-machine(`50000/3000ms`)와 distributed(`5000/3000ms`) 간 반영 지연/처리량 특성이 달라집니다. |
| **Raw/Serving 분리** | sink 안정성(at-least-once)과 조회 정합성(중복 완화)을 분리하기 위함입니다. | serving 레이어(`MV + FINAL`) 운영 비용이 추가됩니다. |

## Failure Modes & Handling
| Failure | Detection | Response |
| :--- | :--- | :--- |
| **Deserialization Error** | `SafeTaxiEventSchema`의 `catch` 블록에서 감지됩니다. | 에러 로그 샘플(`DESER_ERROR`) + `DESERIALIZATION_FAILED` DLQ(JSONL) 기록 후 skip 처리합니다. |
| **Required Field / ts Invalid** | raw stream filter(`trip_id`/`ts` null, `Instant.parse` 실패)에서 감지됩니다. | `VALIDATION_FAILED` DLQ(JSONL) 기록 후 drop됩니다. |
| **Late/Out-of-order** | `eventTs <= wm` 또는 재정렬 시 `ts < lastEmittedTs` 조건으로 감지됩니다. | `LATE_EVENTS` 사이드 아웃풋으로 분리되고, 동시에 `LATE_DROPPED` DLQ(JSONL) 기록 후 메인 파이프라인에서 제외됩니다. |
| **DLQ 경로 권한/마운트 문제** | `/opt/flink/data` 초기화 실패 로그에서 감지됩니다. | `/tmp/dead_letter_queue-<hostname>.jsonl` fallback 경로를 사용해 기록을 지속합니다. |
| **Sink Retry** | JDBC Sink 내부 예외 발생 시 감지됩니다. | 최대 5회까지 재시도(`withMaxRetries(5)`)하며 복구를 시도합니다. |
| **Raw 중복 증가** | `taxi_events`/`taxi_predictions`의 동일 키 duplicate group 쿼리에서 감지됩니다. | 운영 조회/대시보드는 `taxi_events_latest`/`taxi_predictions_latest`를 기준으로 사용합니다. |
| **ONNX 모델 파일 경로 누락** | `Failed to initialize ONNX session` 런타임 예외로 감지됩니다. | 모든 Flink 실행 노드에 `FLINK_ONNX_MODEL_PATH` 경로가 동일하게 존재하도록 볼륨 마운트를 맞춥니다. |
| **Prediction 미생성** | `taxi_events`는 증가하지만 `taxi_predictions`가 0건인 경우 | `lag_20` 누적 여부, ONNX 로드 로그, prediction sink env(`FLINK_ENABLE_PREDICTION_SINK`)를 점검합니다. |
