# Ingestor Logging Guide

## 1) 목적
이 문서는 Ingestor 로깅을 사용자 인사이트(1~7)에 맞춰 정리한 운영 기준이다.

핵심 목표:
- Sink 백프레셔 조기 감지 (`FAIL_OVERFLOW`)
- 동시 emit 경합 추이 확인 (`FAIL_NON_SERIALIZED`)
- Flux 파이프라인 생존성/반응성 확인
- Kafka 전송 throughput 및 in-flight(ACK 대기) 상태 추적
- 직렬화 실패 이벤트의 DLQ 적재 상태 추적
- 재시도 시도량/소진량 추적
- HTTP 수용 상태(202/429/5xx)와 내부 상태를 함께 관찰해 병목 위치를 빠르게 분해

---

## 2) 기본 원칙
- `INFO`: 10초 주기 요약 로그(기본)
- `WARN`: 임계치 초과, overflow 스파이크, retry 급증
- `ERROR`: 배치 전송 실패 누적, DLQ write 실패, 파이프라인 비정상 종료
- `DEBUG`: 샘플링 상세(상시 활성화 금지)

권장:
- 이벤트 1건당 상세 로그 상시 출력 금지
- “주기 요약 + 임계치 경보 + 샘플 상세” 조합 사용

운영 고정값(기본):
- `window_sec=10`
- `warn_consecutive_windows=3` (30초)
- `error_consecutive_windows=6` (60초)
- `debug_sample_rate=0.1%` (기본), 장애 분석 시 최대 1%까지 일시 상향
- 모든 요약 로그에 `service`, `instance`, `topic`, `window_sec`, `ts` 포함

---

## 3) 로깅 항목 (사용자 인사이트 기준)

### 3.1 현재 내부 SINK에 얼마나 쌓였는지
목적:
- 수신 속도와 Kafka 처리 속도의 격차(적체)를 확인

필수 로그(`INFO`, 10초):
- `events_received_delta_10s`
- `events_processed_delta_10s`
- `events_failed_delta_10s`
- `sink_pending_estimate = max(0, events_received_total - events_processed_total - events_failed_total - events_dropped_total)`
- `sink_util_estimate_pct`
- `events_dropped_delta_10s`
- `buffer_full_events_delta_10s`

경보(`WARN`):
- `sink_util_estimate_pct >= 80%`가 3회 연속
- `events_dropped_delta_10s > 0`가 2회 연속

경보(`ERROR`):
- `sink_util_estimate_pct >= 95%`가 6회 연속
- `events_dropped_delta_10s >= 1000` (단일 윈도우)

### 3.2 `FAIL_NON_SERIALIZED` / `FAIL_OVERFLOW` 로깅
목적:
- 동시성 경합과 실제 백프레셔를 분리해서 관찰

필수 로그:
- 주기 집계(`INFO`)
  - `emit_total_delta_10s`
  - `emit_ok_delta_10s`
  - `emit_non_serialized_delta_10s`
  - `emit_overflow_delta_10s`
  - `emit_other_fail_delta_10s` (`FAIL_CANCELLED`, `FAIL_TERMINATED`, `FAIL_ZERO_SUBSCRIBER` 등)
  - `emit_non_serialized_rate = emit_non_serialized_delta_10s / max(1, emit_total_delta_10s)`
  - `emit_overflow_rate = emit_overflow_delta_10s / max(1, emit_total_delta_10s)`
- 이벤트성
  - `FAIL_NON_SERIALIZED`: 샘플링 `DEBUG` 또는 rate 기반 `WARN`
  - `FAIL_OVERFLOW`: `WARN` 유지

관점:
- `FAIL_NON_SERIALIZED`는 치명 알람보다 추이 관찰 중심
- `FAIL_OVERFLOW`는 중복 로그가 생길 수 있으므로 이벤트 로그 + 주기 집계 역할을 분리

경보 기준:
- `emit_non_serialized_rate >= 1.0%` 3회 연속 -> `WARN`
- `emit_non_serialized_rate >= 5.0%` 6회 연속 -> `ERROR`
- `emit_overflow_rate > 0` 2회 연속 -> `WARN`
- `emit_overflow_rate >= 2.0%` 3회 연속 -> `ERROR`

### 3.3 Flux가 어떻게 반응하는지 확인할지
목적:
- 파이프라인이 살아 있고 실제 소비/전송 중인지 확인

기본 로그(`INFO`):
- 주기(`INFO`)
  - `pipeline_last_batch_at`
  - `pipeline_idle_sec`
  - `pipeline_resubscribe_total` (`.retry()` 재구독 횟수)
  - `pipeline_error_total`
  - `pipeline_stall_windows`
- 필요 시 상세 로그(`DEBUG`)
  - 샘플링 배치 시작/종료(`batch_size`, `duration_ms`, `signal`)

판단:
- 운영에서는 heartbeat 지표는 유지
- 상세 flux trace는 이해/장애 분석이 필요할 때만 한시 활성화

경보 기준:
- `events_received_delta_10s > 0` 이고 `events_processed_delta_10s = 0`이 3회 연속 -> `WARN`
- 위 조건이 6회 연속 -> `ERROR`
- `pipeline_resubscribe_total` 증가 감지 시 이벤트 `WARN` 1회 출력

### 3.4 Flux -> Kafka 전송 throughput
목적:
- 실시간 처리량과 배치 효율 확인

필수 로그(`INFO`, 10초):
- `events_processed_delta_10s`
- `events_per_sec`
- `batches_sent_delta_10s`
- `avg_batch_size = processed_delta / max(1, batches_delta)`
- `flush_by_size_delta_10s`
- `flush_by_timeout_delta_10s`
- `flush_by_timeout_ratio = flush_by_timeout_delta_10s / max(1, batches_sent_delta_10s)`
- `batch_total_ms_p50/p95`

권장(병목 분해용) 로그:
- `queue_wait_ms_p50/p95`
- `serialize_ms_p50/p95`
- `kafka_ack_ms_p50/p95`

경보 기준:
- `events_per_sec`가 5분 기준선 대비 50% 이상 하락 + 3회 연속 -> `WARN`
- `events_per_sec`가 5분 기준선 대비 80% 이상 하락 + 6회 연속 -> `ERROR`
- `flush_by_timeout_ratio >= 80%` 3회 연속 -> `WARN` (배치 효율 저하)
- `kafka_ack_ms_p95 >= 300`ms 3회 연속 -> `WARN`
- `kafka_ack_ms_p95 >= 1000`ms 6회 연속 -> `ERROR`

### 3.5 ACK 대기 중 요청 수
목적:
- sender 포화 여부와 지연 상승 위험 감지

제약:
- Reactor Kafka에서 “정확한 ACK 대기 건수”를 바로 노출하지 않음

필수 로그(추정치 기반):
- 카운터 추가
  - `records_enqueued_total` (send 직전)
  - `records_completed_total` (SenderResult 수신 시)
  - `records_inflight_estimate = enqueued - completed`
- 주기(`INFO`)
  - `records_inflight_estimate`
  - `sender_max_inflight_config` (현재 1024)
  - `records_inflight_util_pct`

경보(`WARN`):
- `records_inflight_util_pct >= 80%` 3회 연속

경보(`ERROR`):
- `records_inflight_util_pct >= 95%` 6회 연속

### 3.6 DLQ 적재 이벤트 로깅
목적:
- 직렬화 실패 이벤트의 격리/누락 여부 확인

필수 로그:
- 주기(`INFO`)
  - `dlq_written_delta_10s`
  - `dlq_total_written`
  - `serialization_failed_delta_10s`
  - `dlq_write_error_delta_10s`
  - `dlq_file_size_bytes`
- 이벤트성
  - `DLQ write success`: 샘플링 `DEBUG`
  - `DLQ write failure`: `ERROR` 유지

경보 기준:
- `dlq_write_error_delta_10s > 0` -> 즉시 `ERROR`
- `dlq_written_delta_10s > 0` 3회 연속 -> `WARN`

### 3.7 재시도 중인 건수/상태
목적:
- transient 장애 시 recovery 동작량 파악

필수 로그:
- 주기(`INFO`)
  - `retry_attempts_delta_10s`
  - `retry_batches_delta_10s`
  - `retry_exhausted_delta_10s`
  - `retry_attempt_rate = retry_attempts_delta_10s / max(1, retry_batches_delta_10s)`
  - (가능하면) `retry_inflight_estimate`
- 이벤트성
  - `retry_exhausted`: `ERROR`

경보 기준:
- `retry_attempt_rate >= 0.3` 3회 연속 -> `WARN`
- `retry_exhausted_delta_10s > 0` -> 즉시 `ERROR`

### 3.8 HTTP 수용 상태 (입구 관점)
목적:
- 현재 상태를 외부 요청 관점에서 즉시 파악
- 병목 위치를 `HTTP 입구` vs `내부 파이프라인`으로 분리

필수 로그(`INFO`, 10초):
- `http_requests_delta_10s{endpoint=/ingest,status=202|429|503|500}`
- `http_requests_delta_10s{endpoint=/ingest/batch,status=202|207|429|500|400}`
- `http_429_ratio`
- `http_5xx_ratio`
- `http_latency_ms_p50/p95{endpoint}`

경보 기준:
- `http_429_ratio >= 5%` 3회 연속 -> `WARN`
- `http_429_ratio >= 20%` 3회 연속 -> `ERROR`
- `http_5xx_ratio >= 1%` 3회 연속 -> `WARN`
- `http_5xx_ratio >= 5%` 3회 연속 -> `ERROR`

### 3.9 JVM/컨테이너 리소스 (성능 원인 분해)
목적:
- 처리율 저하가 앱 로직인지 리소스 한계인지 분리

필수 로그(`INFO`, 10초):
- `heap_used_pct`
- `gc_pause_ms_p95`
- `process_cpu_pct`
- `container_memory_used_pct`
- `container_cpu_used_pct`

경보 기준:
- `heap_used_pct >= 85%` 3회 연속 -> `WARN`
- `heap_used_pct >= 92%` 6회 연속 -> `ERROR`
- `gc_pause_ms_p95 >= 200`ms 3회 연속 -> `WARN`
- `gc_pause_ms_p95 >= 500`ms 6회 연속 -> `ERROR`

---

## 4) 정합성 검증 식 (필수)
운영 시 아래 식이 장시간 어긋나면 계측 누락/중복 또는 경로 누수를 점검한다.

- `emit_total_delta_10s = emit_ok_delta_10s + emit_non_serialized_delta_10s + emit_overflow_delta_10s + emit_other_fail_delta_10s`
- `records_inflight_estimate(t) = records_inflight_estimate(t-1) + records_enqueued_delta_10s - records_completed_delta_10s`
- `events_received_delta_10s >= events_processed_delta_10s + events_failed_delta_10s + events_dropped_delta_10s` (관측 지연 고려)
- `dlq_written_delta_10s <= serialization_failed_delta_10s`

---

## 4.1 현재 Prometheus 매핑 (2026-02-22)
Ingestor는 Spring Actuator + Micrometer Prometheus registry를 사용한다.

- endpoint:
  - `/actuator/prometheus`
  - `/actuator/health`
- 기본 metric:
  - `http_server_requests_seconds_*`
  - `jvm_*`, `process_*`, `system_*`
- 커스텀 metric(현재 구현):
  - `ingestor_events_received_total`
  - `ingestor_events_processed_total`
  - `ingestor_events_failed_total`
  - `ingestor_events_dropped_total`
  - `ingestor_batches_sent_total`
  - `ingestor_emit_total`
  - `ingestor_emit_non_serialized_total`
  - `ingestor_emit_overflow_total`
  - `ingestor_emit_other_fail_total`
  - `ingestor_kafka_records_enqueued_total`
  - `ingestor_kafka_records_completed_total`
  - `ingestor_retry_attempts_total`
  - `ingestor_retry_exhausted_total`
  - `ingestor_dlq_write_errors_total`
  - `ingestor_pipeline_resubscribe_total`
  - `ingestor_pipeline_errors_total`
  - `ingestor_sink_buffer_usage_percent`
  - `ingestor_sink_pending_estimate`
  - `ingestor_kafka_records_inflight_estimate`
  - `ingestor_kafka_sender_max_inflight`
  - `ingestor_dlq_total_written`
  - `ingestor_kafka_batch_send_duration_seconds_*`
  - `ingestor_kafka_batch_size_*`

운영 쿼리 시작점(예시):
- 수신량(10s): `rate(ingestor_events_received_total[10s])`
- 처리량(10s): `rate(ingestor_events_processed_total[10s])`
- 드롭량(10s): `rate(ingestor_events_dropped_total[10s])`
- in-flight 추정: `ingestor_kafka_records_inflight_estimate`
- 배치 지연 p95(5m): `histogram_quantile(0.95, sum(rate(ingestor_kafka_batch_send_duration_seconds_bucket[5m])) by (le))`

---

## 5) 권장 출력 예시
```text
[ING_METRICS] ts=... service=ingestor instance=ingestor-1 topic=taxi-event-data win=10s recv_d=120000 proc_d=118500 fail_d=320 drop_d=180 sink_pending=1500 sink_util=15.0% emit_total_d=120300 emit_non_serialized_d=240 emit_overflow_d=180 batches_d=240 eps=11850 avg_batch=493.8 queue_wait_p95=22 serialize_p95=9 kafka_ack_p95=180 inflight_est=410/1024 inflight_util=40.0% retry_attempts_d=27 retry_exhausted_d=1 dlq_d=14 dlq_total=241 http429_ratio=1.8% http5xx_ratio=0.0%
```

```text
[ING_WARN] type=sink_saturation ts=... win=10s sink_util=95.0% overflow_rate=2.7% consecutive=3 action=throttle_generator_and_check_kafka_ack
```

```text
[ING_ERROR] type=retry_exhausted ts=... batch_size=500 retry_attempts=3 root_cause=TimeoutException action=check_kafka_broker_and_network
```

```text
[ING_ERROR] type=dlq_write_failed ts=... trip_id=123456 error=IOException message="No space left on device"
```

---

## 6) 운영 체크리스트
- `sink_pending_estimate`가 장시간 증가하는가?
- `emit_overflow_delta_10s`가 0으로 회복되지 않는가?
- `emit_non_serialized_delta_10s`가 급증하는가?
- `events_per_sec`가 급락했는가?
- `records_inflight_estimate`가 상한 근처에서 내려오지 않는가?
- `retry_exhausted_delta_10s`가 증가하는가?
- `dlq_written_delta_10s`가 증가하는가?
- `http_429_ratio` 또는 `http_5xx_ratio`가 임계 이상으로 유지되는가?
- `kafka_ack_ms_p95`와 `records_inflight_util_pct`가 동시에 상승하는가?
- `heap_used_pct`와 `gc_pause_ms_p95` 상승이 처리율 저하와 동행하는가?
