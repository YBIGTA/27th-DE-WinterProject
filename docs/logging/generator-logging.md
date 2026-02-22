# Generator Logging Guide

## 1) 목적
이 문서는 Generator의 전송 상태를 운영 관점에서 추적하기 위한 로깅 기준을 정의한다.

핵심 운영 질문:
1. 지금 Generator가 정상적으로 이벤트를 생산/전송하고 있는가?
2. 병목은 어디에서 발생하는가? (`scheduler` / `payload_queue` / `requeue` / HTTP 전송)
3. 처리량/지연 성능이 목표 범위 내에 있는가?
4. 손실 경로(`drop`, `DLQ`, `CB`, `429`, `5xx`, `status=0`)가 악화되고 있는가?

---

## 2) 기본 원칙
- `INFO`: 주기 요약 로그(기본 10초, `win=10s`)
- `WARN`: 임계치 초과/지속, 성능 급락, 재시도 경로 악화
- `ERROR`: 손실 경로 증가(DLQ/드롭), 회복 불가 상태 지속
- `DEBUG`: 샘플링 상세 로그(상시 비활성)

운영 원칙:
- “이벤트 전건 상세” 금지
- “주기 집계 + 임계치 경보 + 샘플 상세” 조합
- 핵심 카운터는 `누적(total)` + `구간(delta_10s)`를 함께 기록
- 모든 주기 로그에 공통 필드 포함:
  - `ts`, `run_id`, `component=generator`, `win=10s`

---

## 3) 로깅 항목

### 3.1 시작 스냅샷 (상태 기준선)
목적:
- 실행 시점 설정/적재 상태를 기준선으로 남긴다.

권장 로그:
- 시작 1회(`INFO`)
  - `discovered_files`
  - `loaded_rows_total`
  - `seed_pickup_count`
  - `seed_dropoff_count`
  - `event_queue_size_after_seed`
  - `playback_speed`
  - `batch_size`, `batch_timeout_ms`
  - `sender_count`
  - `payload_queue_capacity`, `requeue_capacity`
  - `rate_limit_threshold`, `rate_limit_max_delay_ms`
  - `circuit_breaker_threshold`, `circuit_breaker_min_requests`, `circuit_breaker_timeout_sec`

### 3.2 스케줄링 상태 (현재 진행률)
목적:
- 이벤트 생성이 정상 진행 중인지 확인한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `event_queue_remaining`
  - `events_scheduled_delta_10s`
  - `in_transit_generated_total`
  - `scheduler_push_block_ms_sum_10s` (가능하면)
  - `scheduler_push_block_count_delta_10s` (가능하면)

메모:
- `event_queue_remaining` 감소가 정지했는데 `payload_queue_util_pct`가 높으면 scheduler backpressure 가능성이 높다.

### 3.3 `payload_queue` 상태 (1차 병목)
목적:
- 전송 경로 백프레셔를 조기 감지한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `payload_queue_depth`
  - `payload_queue_util_pct`
  - `payload_queue_high_watermark`
  - `payload_queue_wait_ms_p95` (가능하면)

이벤트성(`WARN`):
- `payload_queue_util_pct` 고수준 지속
- `payload_queue_high_watermark` 갱신 빈도 급증

### 3.4 배치 전송 품질 (처리량/지연 핵심)
목적:
- Generator 전송 성능과 배치 효율을 확인한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `batches_sent_delta_10s`
  - `events_sent_success_delta_10s`
  - `events_per_sec`
  - `avg_batch_size`
  - `flush_by_size_count_delta_10s`
  - `flush_by_timeout_count_delta_10s`
  - `flush_by_final_count_delta_10s`
  - `http_latency_ms_p50`, `http_latency_ms_p95`, `http_latency_ms_p99` (가능하면)
  - `batch_wait_ms_p50`, `batch_wait_ms_p95` (가능하면)

샘플 상세(`DEBUG`, N개 중 1개):
- `batch_size`
- `sender_thread_id`
- `flush_reason(size|timeout|final)`
- `status_code`
- `request_latency_ms`

### 3.5 상태코드/재시도/DLQ/드롭 (손실 경로)
목적:
- 실패 원인과 손실 경로를 분리 관찰한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `status_2xx_delta_10s`
  - `status_4xx_non429_delta_10s`
  - `status_429_delta_10s`
  - `status_5xx_delta_10s`
  - `status_0_socket_error_delta_10s`
  - `requeue_depth`
  - `requeue_enqueued_delta_10s`
  - `requeue_dequeued_delta_10s`
  - `requeue_age_ms_p95` (가능하면)
  - `dlq_writes_delta_10s`
  - `dlq_writes_total`
  - `dropped_non_retryable_delta_10s`

이벤트성(`WARN`/`ERROR`):
- `requeue_push_failed` (큐 포화)
- `retry_exhausted_to_dlq`
- `batch_client_error_dropped` (4xx non-429)

메모:
- Generator는 `status=0`(socket/connect/read 실패)를 직접 관측하는 유일 지점이다.

### 3.6 Rate Limiter / Circuit Breaker 상태
목적:
- 보호장치가 얼마나 강하게 개입 중인지 파악한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `rate_limit_delay_ms_current`
  - `rate_limit_429_rate`
  - `rate_limit_total_429s`
  - `cb_state`
  - `cb_trips_total`
  - `cb_rejects_total`
  - `cb_open_duration_sec` (가능하면)

이벤트성(`WARN`):
- `delay_increased` (old -> new)
- `delay_decreased` (old -> new)
- `cb_transition` (`CLOSED->OPEN`, `OPEN->HALF_OPEN`, `HALF_OPEN->CLOSED`)

### 3.7 이벤트 보존식 (누락/계측 이상 탐지)
목적:
- "조용한 누락"을 빠르게 탐지한다.

권장 지표:
- `events_accounting_gap_delta_10s`

권장 계산식:
- `events_scheduled_delta_10s ~= events_sent_success_delta_10s + dropped_non_retryable_delta_10s + dlq_writes_delta_10s + requeue_depth_delta_10s`

메모:
- `requeue_depth_delta_10s = requeue_depth_now - requeue_depth_prev`
- 일시적인 미세 오차는 허용하되, gap 지속 시 `WARN` 권장

### 3.8 프로세스 리소스 (성능 원인 분리)
목적:
- 처리량 저하가 CPU/메모리/스레드 포화 때문인지 분리한다.

권장 로그:
- 주기(`INFO`, 10초)
  - `proc_cpu_used_pct`
  - `proc_rss_mb`
  - `sender_threads_active`
  - `scheduler_loop_delay_ms_p95` (가능하면)

---

## 4) 메트릭 네이밍/단위 규칙
- `*_total`: 프로세스 시작 이후 단조 증가 카운터
- `*_delta_10s`: 10초 구간 증분
- `*_pct`: 0~100 퍼센트
- `*_ms`, `*_us`, `*_sec`: 단위 고정
- `*_p50/p95/p99`: 백분위 지연

운영 권장:
- 핵심 카운터는 `total + delta_10s` 둘 다 기록
- 측정 불가 값은 생략 대신 `na` 또는 명시적 필드로 표기

---

## 4.1 Prometheus 정석 구현 명세 (native `/metrics`)
Generator는 로그 전용이 아니라 native endpoint로 Prometheus를 직접 노출한다.

Endpoint 계약:
- bind: `0.0.0.0:${GENERATOR_METRICS_PORT:-9108}`
- path: `GET /metrics`
- format: Prometheus text/OpenMetrics
- 응답 목표: `p95 < 100ms`, payload body 미포함

필수 카운터:
- `generator_events_scheduled_total`
- `generator_events_sent_success_total`
- `generator_batches_sent_total`
- `generator_http_requests_total{code_class}`
- `generator_http_socket_errors_total`
- `generator_requeue_enqueued_total`
- `generator_requeue_dequeued_total`
- `generator_retry_exhausted_total`
- `generator_dlq_writes_total`
- `generator_dropped_non_retryable_total`
- `generator_rate_limit_429_total`
- `generator_cb_trips_total`
- `generator_cb_rejects_total`

필수 게이지:
- `generator_event_queue_remaining`
- `generator_payload_queue_depth`
- `generator_payload_queue_capacity`
- `generator_payload_queue_utilization_ratio`
- `generator_requeue_depth`
- `generator_requeue_capacity`
- `generator_requeue_utilization_ratio`
- `generator_rate_limit_delay_seconds`
- `generator_circuit_breaker_state` (`0=CLOSED`, `1=HALF_OPEN`, `2=OPEN`)
- `generator_sender_threads_active`

필수 히스토그램:
- `generator_http_request_duration_seconds`
- `generator_batch_wait_duration_seconds`
- `generator_scheduler_push_block_duration_seconds` (가능 시)

라벨 정책:
- 허용: 저카디널리티 라벨만(`instance`, `code_class`, `flush_reason`)
- 금지: `trip_id`, `request_id`, URL raw path, payload 기반 라벨
- `run_id`는 라벨로 붙이지 말고 로그 필드로만 유지

Prometheus scrape 기준:
- single/distributed 공통으로 `generator` job 추가
- target: `"${GENERATOR_IP}:${GENERATOR_METRICS_PORT:-9108}"`
- scrape interval: `10s` (generator 운영 윈도우와 동일)

PromQL 시작점:
- throughput: `rate(generator_events_sent_success_total[1m])`
- queue util: `generator_payload_queue_utilization_ratio`
- requeue depth: `generator_requeue_depth`
- error ratio: `(rate(generator_http_socket_errors_total[1m]) + rate(generator_retry_exhausted_total[1m])) / clamp_min(rate(generator_http_requests_total[1m]), 1)`
- batch wait p95: `histogram_quantile(0.95, sum(rate(generator_batch_wait_duration_seconds_bucket[5m])) by (le))`

## 4.2 Loki 보완 범위 (정석 분리)
Prometheus에서 탐지한 이상 신호의 원인 분석은 Loki로 수행한다.

필수 수집 이벤트:
- `type=requeue_push_failed`
- `type=retry_exhausted_to_dlq`
- `type=batch_client_error_dropped`
- `type=cb_transition`
- `type=delay_increased|delay_decreased`
- `status=0` socket/connect/read 실패 샘플

필수 라벨/필드:
- label: `service=generator`, `level`, `type`
- field: `run_id`, `stage`, `status_code`, `flush_reason`, `action`

LogQL 시작점:
- 최근 재시도 소진: `{service="generator"} |= "retry_exhausted_to_dlq"`
- CB 상태 전이: `{service="generator"} |= "cb_transition"`
- 소켓 오류 샘플: `{service="generator"} |= "status_0_socket_error"`

---

## 5) 임계치 초안 (운영 시작값)
- `payload_queue_util_pct >= 80%` 60초 지속 -> `WARN`
- `payload_queue_util_pct >= 95%` 30초 지속 -> `ERROR`
- `requeue_depth` 5개 윈도우 연속 증가 -> `WARN`
- `requeue_depth >= 70% * requeue_capacity` 30초 지속 -> `ERROR`
- `dlq_writes_delta_10s > 0` 3회 연속 -> `ERROR`
- `dropped_non_retryable_delta_10s > 0` -> `WARN`
- `status_429_delta_10s / total_requests_delta_10s >= 10%` 60초 지속 -> `WARN`
- `status_429_delta_10s / total_requests_delta_10s >= 20%` 120초 지속 -> `ERROR`
- `(status_5xx_delta_10s + status_0_socket_error_delta_10s) / total_requests_delta_10s >= 5%` 60초 지속 -> `WARN`
- `(status_5xx_delta_10s + status_0_socket_error_delta_10s) / total_requests_delta_10s >= 15%` 60초 지속 -> `ERROR`
- `cb_state=OPEN` 상태 30초 이상 -> `WARN`
- `cb_state=OPEN` 상태 120초 이상 -> `ERROR`
- `events_per_sec`가 5분 baseline 대비 30% 하락 60초 지속 -> `WARN`
- `events_per_sec`가 5분 baseline 대비 50% 하락 120초 지속 -> `ERROR`
- `abs(events_accounting_gap_delta_10s) > max(100, events_scheduled_delta_10s * 0.5%)` 3회 연속 -> `WARN`

주의:
- 시작값이며, 실제 부하 테스트 후 튜닝 필요

---

## 6) 권장 출력 예시
```text
[GEN_METRICS] ts=... run_id=gen-20260221-01 component=generator win=10s event_q_remaining=321000 scheduled_delta=84000 payload_q=512 payload_q_util=12.5 requeue_q=4 requeue_enq=12 requeue_deq=10 dlq_delta=0 dropped_non_retryable_delta=1 batches_delta=420 events_success_delta=83999 eps=8400 avg_batch=200.0 flush_size_delta=390 flush_timeout_delta=30 status_2xx_delta=418 status_4xx_non429_delta=1 status_429_delta=1 status_5xx_delta=0 status_0_delta=0 cb_state=CLOSED cb_trips=0 cb_rejects=0 rl_delay_ms=15 rl_429_rate=0.03 accounting_gap=0
```

```text
[GEN_WARN] ts=... run_id=... type=payload_queue_hot payload_queue_util_pct=88.2 duration_sec=75 action=reduce_playback_or_increase_sender_capacity
```

```text
[GEN_ERROR] ts=... run_id=... type=dlq_growth dlq_writes_delta_10s=42 consecutive_windows=3 action=check_ingestor_http_errors_and_replay_strategy
```

---

## 7) 운영 체크리스트
- `event_queue_remaining`가 감소하고 `events_per_sec`가 안정적인가?
- `payload_queue_util_pct`/`requeue_depth`가 수렴하는가?
- `status_429`, `status_5xx`, `status_0`가 임계치 이하인가?
- `dlq_writes_delta_10s`, `dropped_non_retryable_delta_10s`가 0 또는 낮은 수준인가?
- `cb_state`가 대부분 `CLOSED`를 유지하는가?
- `events_accounting_gap_delta_10s`가 허용 범위 내인가?
- `proc_cpu_used_pct`/`proc_rss_mb`가 성능 저하 시점과 동행하는가?

---

## 8) 구현 메모 (현재 코드 기준)
- 정석안은 textfile collector를 사용하지 않고 native `/metrics` endpoint를 직접 노출한다.
- `BoundedQueue` depth/high-watermark는 atomic 카운터로 계측 포인트를 명시 추가한다.
- `batch_wait_ms` 과소 계측 이슈를 막기 위해 enqueue 시각/flush 시각 측정을 히스토그램으로 고정한다.
- `status_*_delta_10s`, `dropped_non_retryable_delta_10s`, `events_accounting_gap_delta_10s`는 Prometheus 카운터/게이지 원천값(`*_total`, depth gauge)으로 대체 가능하게 구성한다.
