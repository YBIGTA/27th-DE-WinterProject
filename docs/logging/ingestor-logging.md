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

---

## 2) 기본 원칙
- `INFO`: 10초 주기 요약 로그(기본)
- `WARN`: 임계치 초과, overflow 스파이크, retry 급증
- `ERROR`: 배치 전송 실패 누적, DLQ write 실패, 파이프라인 비정상 종료
- `DEBUG`: 샘플링 상세(상시 활성화 금지)

권장:
- 이벤트 1건당 상세 로그 상시 출력 금지
- “주기 요약 + 임계치 경보 + 샘플 상세” 조합 사용

---

## 3) 로깅 항목 (사용자 인사이트 기준)

### 3.1 현재 내부 SINK에 얼마나 쌓였는지
목적:
- 수신 속도와 Kafka 처리 속도의 격차(적체)를 확인

필수 로그(`INFO`, 10초):
- `sink_pending_estimate = events_received - events_processed`
- `sink_util_estimate_pct`
- `events_dropped_delta_10s`
- `buffer_full_events_delta_10s`

경보(`WARN`):
- `sink_util_estimate_pct >= 80%`가 N회 연속
- `events_dropped_delta_10s > 0` 지속

### 3.2 `FAIL_NON_SERIALIZED` / `FAIL_OVERFLOW` 로깅
목적:
- 동시성 경합과 실제 백프레셔를 분리해서 관찰

필수 로그:
- 주기 집계(`INFO`)
  - `emit_ok_delta_10s`
  - `emit_non_serialized_delta_10s`
  - `emit_overflow_delta_10s`
- 이벤트성
  - `FAIL_NON_SERIALIZED`: 샘플링 `DEBUG` 또는 rate 기반 `WARN`
  - `FAIL_OVERFLOW`: `WARN` 유지

관점:
- `FAIL_NON_SERIALIZED`는 치명 알람보다 추이 관찰 중심
- `FAIL_OVERFLOW`는 중복 로그가 생길 수 있으므로 이벤트 로그 + 주기 집계 역할을 분리

### 3.3 Flux가 어떻게 반응하는지 확인할지
목적:
- 파이프라인이 살아 있고 실제 소비/전송 중인지 확인

기본 로그(`INFO`):
- 주기(`INFO`)
  - `pipeline_last_batch_at`
  - `pipeline_idle_sec`
  - `pipeline_resubscribe_total` (`.retry()` 재구독 횟수)
  - `pipeline_error_total`
- 필요 시 상세 로그(`DEBUG`)
  - 샘플링 배치 시작/종료(`batch_size`, `duration_ms`, `signal`)

판단:
- 운영에서는 heartbeat 지표는 유지
- 상세 flux trace는 이해/장애 분석이 필요할 때만 한시 활성화

### 3.4 Flux -> Kafka 전송 throughput
목적:
- 실시간 처리량과 배치 효율 확인

필수 로그(`INFO`, 10초):
- `events_processed_delta_10s`
- `events_per_sec`
- `batches_sent_delta_10s`
- `avg_batch_size = processed_delta / max(1, batches_delta)`
- 선택 로그: `batch_duration_ms_p50/p95`

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

경보(`WARN`):
- `records_inflight_estimate >= 80% * sender_max_inflight_config` 지속

### 3.6 DLQ 적재 이벤트 로깅
목적:
- 직렬화 실패 이벤트의 격리/누락 여부 확인

필수 로그:
- 주기(`INFO`)
  - `dlq_written_delta_10s`
  - `dlq_total_written`
- 이벤트성
  - `DLQ write success`: 샘플링 `DEBUG`
  - `DLQ write failure`: `ERROR` 유지

### 3.7 재시도 중인 건수/상태
목적:
- transient 장애 시 recovery 동작량 파악

필수 로그:
- 주기(`INFO`)
  - `retry_attempts_delta_10s`
  - `retry_batches_delta_10s`
  - `retry_exhausted_delta_10s`
  - (가능하면) `retry_inflight_estimate`
- 이벤트성
  - `retry_exhausted`: `ERROR`

---

## 4) 추가 권장 항목
- `records_inflight_estimate` 카운터 도입
- pipeline heartbeat/idle/resubscribe 지표 도입
- overflow 중복 로그 정리(이벤트 로그 + 주기 집계 역할 분리)
- 배치 지연 분포(`p50/p95`) 추가
- 샘플링 정책 자동화(고부하 시 debug 억제)

---

## 5) 권장 출력 예시
```text
[ING_METRICS] ts=... win=10s recv=120000 proc=118500 fail=320 drop=180 sink_pending=1500 sink_util_est=15.0% batches=240 eps=11850 avg_batch=493.8 inflight_est=410/1024 retry_attempts=27 retry_exhausted=1 dlq_delta=14 dlq_total=241
```

```text
[ING_WARN] type=overflow_spike win=10s emit_overflow=180 sink_util_est=95.0% action=check_generator_rate_and_kafka_latency
```

```text
[ING_ERROR] type=dlq_write_failed trip_id=123456 error=IOException message="No space left on device"
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
