# Generator Logging Guide

## 1) 목적
이 문서는 Generator의 전송 상태를 운영 관점에서 추적하기 위한 로깅 기준을 정의한다.

핵심 목표:
- Parquet 로딩/스케줄링이 정상인지 확인
- 전송 병목(`payload_queue`, `requeue`)을 조기 감지
- 배치 전송 품질(크기/주기/처리량) 확인
- 장애 대응 경로(`429`, `5xx`, `CB`, `DLQ`)를 빠르게 파악

---

## 2) 기본 원칙
- `INFO`: 주기 요약 로그(기본 10초)
- `WARN`: 임계치 초과, 재시도 급증, 429 급증
- `ERROR`: DLQ 증가, 배치 전송 실패 누적, 예상치 못한 오류
- `DEBUG`: 샘플링된 상세 로그(항상 켜지 않음)

권장:
- “모든 이벤트 상세 로그” 금지
- “요약 + 임계치 경보 + 샘플 상세” 조합 사용

---

## 3) 로깅 항목 (요청안 기준)

### 3.1 Parquet 로딩 및 초기 큐 적재
목적:
- 파일 로딩 성공 여부 확인
- 초기 `event_queue` 적재량 확인

권장 로그:
- 시작 1회(`INFO`)
  - `discovered_files`
  - `loaded_rows_total`
  - `seed_pickup_count`
  - `seed_dropoff_count`
  - `event_queue_size_after_seed`
- 주기(`INFO`, 10초)
  - `event_queue_remaining`
  - `in_transit_generated_total`

메모:
- 초기 1회 스냅샷 + 주기 상태 둘 다 필요
- `IN_TRANSIT`는 런타임에서 계속 증가하므로 주기 로그가 유효함

### 3.2 `payload_queue` 상태
목적:
- 전송 병목/백프레셔 감지

권장 로그:
- 주기(`INFO`, 10초)
  - `payload_queue_depth`
  - `payload_queue_util_pct`
  - `payload_queue_high_watermark`
  - `scheduler_push_block_ms_sum` (가능하면)

경보(`WARN`):
- `payload_queue_util_pct >= 80%` 일정 시간 지속

### 3.3 배치 전송 상태
목적:
- 배치가 실제로 원하는 형태로 전송되는지 확인
- generator 측 전송 throughput 산출

권장 로그:
- 주기 집계(`INFO`, 10초)
  - `batches_sent`
  - `events_sent`
  - `events_per_sec`
  - `avg_batch_size`
  - `flush_by_size_count`
  - `flush_by_timeout_count`
  - `http_latency_ms_p50` / `http_latency_ms_p95` (가능하면)
- 샘플 상세(`DEBUG`, N개 중 1개)
  - `batch_size`
  - `sender_thread_id`
  - `flush_reason(size|timeout|final)`
  - `status_code`
  - `request_latency_ms`

### 3.4 `requeue` / `DLQ` 상태
목적:
- 장애 상황에서 재시도 경로 건전성 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `requeue_depth`
  - `requeue_enqueued`
  - `requeue_dequeued`
  - `dlq_writes_total`
- 이벤트성(`WARN`/`ERROR`)
  - `requeue_push_failed` (큐 포화)
  - `retry_exhausted_to_dlq`

### 3.5 응답 상태코드 분포
목적:
- 현재 전송 상태를 generator 관점에서 파악

권장 로그:
- 주기(`INFO`, 10초)
  - `status_2xx`
  - `status_400`
  - `status_429`
  - `status_5xx`
  - `status_0_socket_error`

메모:
- ingestor 로그와 별개로 generator 쪽 집계가 필요
- generator만이 `status=0`(소켓 오류)를 직접 관측함

### 3.6 Rate Limiter 지연 상태
목적:
- 429 대응이 얼마나 강하게 걸리는지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `rate_limit_delay_ms_current`
  - `rate_limit_429_rate`
  - `rate_limit_total_429s`
- 이벤트성(`WARN`)
  - `delay_increased` (old -> new)
  - `delay_decreased` (old -> new)

---

## 4) 추가 권장 항목
- Circuit Breaker 상태
  - `cb_state`, `cb_trips_total`, `cb_rejects_total`
  - 상태 전이 시점(`CLOSED->OPEN`, `OPEN->HALF_OPEN`, `HALF_OPEN->CLOSED`)
- 실행 시작 설정 스냅샷 1회
  - `playback_speed`, `batch_size`, `batch_timeout_ms`, `sender_count`, queue capacities

---

## 5) 권장 출력 예시
```text
[GEN_METRICS] ts=... files=12 rows=1450000 event_q_remain=321000 in_transit_gen=78000 payload_q=512 payload_q_util=12.5% requeue_q=4 dlq=0 batches=420 events=84000 eps=8400 avg_batch=200.0 status_2xx=418 status_400=1 status_429=1 status_5xx=0 status_0=0 cb=CLOSED cb_trips=0 cb_rejects=0 rl_delay_ms=15 rl_429_rate=0.03
```

```text
[GEN_WARN] type=requeue_full action=write_dlq retry_count=3 payload_size=238
```

---

## 6) 운영 체크리스트
- `payload_queue_util_pct`가 지속적으로 높지 않은가?
- `status_429`가 급증하는가?
- `requeue_depth`가 줄지 않고 누적되는가?
- `dlq_writes_total`이 증가하는가?
- `cb_state`가 `OPEN`에 오래 머무는가?
- `rate_limit_delay_ms_current`가 장시간 높은가?
