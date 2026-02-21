# Flink Logging Guide

## 1) 목적
이 문서는 Flink 파이프라인의 데이터 손실/지연/병목을 운영 관점에서 빠르게 식별하기 위한 로깅 기준이다.

핵심 목표:
- drop/실패 이벤트가 DLQ로 정상 기록되는지 확인
- Kafka source 소비 속도와 지연(lag) 추이 파악
- 워터마크 진행 상태와 late drop 비율 추적
- 재정렬 버퍼/상태 적체 여부 관찰
- `LATE_EVENTS` 발생 이유를 분리 관찰
- JDBC sink(ClickHouse write)의 처리량/재시도/실패 추적

---

## 2) 기본 원칙
- `INFO`: 10초 주기 요약 로그(기본)
- `WARN`: 임계치 초과(지연/late ratio/재시도 급증)
- `ERROR`: DLQ write 실패, sink flush 실패, 지속적 backlog
- `DEBUG`: 이벤트 상세는 샘플링만 허용

권장:
- 이벤트 1건당 상세 로그 상시 출력 금지
- "주기 집계 + 임계치 경보 + 샘플 상세" 조합 사용

---

## 3) 로깅 항목

### 3.1 DLQ 유입/누락 점검 (데이터 새는지 확인)
목적:
- "처리 실패가 조용히 사라지는지"를 방지
- 실패 이벤트가 모두 분류되어 DLQ로 적재되는지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `dlq_total_written`
  - `dlq_written_delta_10s`
  - `dlq_by_category_delta_10s` (`DESERIALIZATION_FAILED`, `VALIDATION_FAILED`, `LATE_DROPPED`)
  - `dlq_by_reason_delta_10s` (`JSON_PARSE_ERROR`, `MISSING_TRIP_ID`, `INVALID_TS`, `WATERMARK_LATE`, `REORDER_LATE` 등)
- 이벤트성
  - `DLQ write failed`: `ERROR`

권장 균형식(운영 확인):
- `source_in_delta ~= main_out_delta + dlq_delta + filtered_drop_delta`
- 위 식이 지속적으로 맞지 않으면 누락 계측 또는 경로 누수 가능성 점검

### 3.2 Kafka source 소비 속도/지연
목적:
- Flink가 실제로 Kafka를 충분히 따라가고 있는지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `source_records_in_delta_10s`
  - `source_records_per_sec`
  - `consumer_lag_total` (외부 `kafka-consumer-groups`/exporter 연계)
  - `consumer_lag_recovery_rate`

경보(`WARN`):
- 입력량 대비 `source_records_per_sec`가 장시간 낮음
- `consumer_lag_total`이 지속 증가

### 3.3 워터마크 건강도 (late/drop와 직결)
목적:
- 이벤트타임 진행이 정상인지, 늦은 데이터가 과도한지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `current_input_watermark_ms`
  - `current_output_watermark_ms`
  - `watermark_lag_ms` (processing time 대비 지연)
  - `late_events_delta_10s`
  - `late_ratio = late_events_delta / reorder_seen_delta`
- 이벤트성(`WARN`)
  - `watermark_stall_detected`
  - `late_ratio_spike`

메모:
- 워터마크 상세를 "이벤트마다" 찍으면 성능 저하 위험이 큼
- 운영 기본은 주기 집계 + 샘플링 경보가 적합

### 3.4 재정렬 버퍼/상태 관찰
목적:
- `PerTripEventTimeReorder` 내부 적체와 cleanup 효과 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `reorder_buffer_size_estimate`
  - `active_trip_keys_estimate`
  - `buffer_flush_count_delta_10s`
  - `flush_emitted_events_delta_10s`
  - `idle_cleanup_count_delta_10s`

경보(`WARN`):
- `reorder_buffer_size_estimate`가 장시간 증가
- `idle_cleanup`이 동작하지 않거나 과도하게 빈번

### 3.5 `LATE_EVENTS` 분류 로깅
목적:
- 어떤 기준으로 late drop 되는지 원인 분해

권장 로그:
- 주기(`INFO`, 10초)
  - `late_by_reason_delta_10s`
    - `WATERMARK_LATE`
    - `REORDER_LATE`
    - `INVALID_EVENT_TS`
- 이벤트성(`DEBUG`, 샘플링)
  - `trip_id`, `event_ts`, `current_watermark`, `last_emitted_ts`

### 3.6 JDBC sink 처리량/오류
목적:
- ClickHouse write 경로 성능과 안정성 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `jdbc_rows_out_delta_10s` (`taxi_events`, `taxi_predictions` 별도)
  - `jdbc_batches_out_delta_10s`
  - `jdbc_flush_latency_ms_p50/p95` (가능 시)
  - `jdbc_retry_delta_10s`
  - `jdbc_error_delta_10s`
- 이벤트성
  - sink flush 실패/재시도 소진: `ERROR`

경보(`WARN`):
- `jdbc_retry_delta_10s` 급증
- `jdbc_rows_out_delta_10s` 급감 + source 입력 유지

### 3.7 Checkpoint 건강도
목적:
- 상태 복구 가능성과 파이프라인 안정성 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `last_completed_checkpoint_age_sec`
  - `checkpoint_completed_delta_10s`
  - `checkpoint_failed_delta_10s`
  - `checkpoint_duration_ms_p50/p95`

경보(`WARN`):
- `last_completed_checkpoint_age_sec`가 체크포인트 주기의 2배 이상으로 지속
- `checkpoint_failed_delta_10s`가 연속 증가

### 3.8 Job restart / failure 추적
목적:
- 장애 발생 시 자동 복구 여부와 반복 실패 패턴 확인

권장 로그:
- 이벤트성(`WARN`/`ERROR`)
  - `job_restart_count_total`
  - `last_failure_exception_class`
  - `last_failure_stage` (source/reorder/onnx/jdbc)
  - `last_failure_message_sample`
- 주기(`INFO`)
  - `restart_count_delta_10s`

경보(`ERROR`):
- 같은 failure class/stage 조합으로 재시작이 반복

### 3.9 Operator backpressure 비율
목적:
- 어느 오퍼레이터에서 실제 병목이 발생하는지 위치 식별

권장 로그:
- 주기(`INFO`, 10초)
  - `source_backpressured_time_pct`
  - `reorder_backpressured_time_pct`
  - `onnx_backpressured_time_pct`
  - `jdbc_backpressured_time_pct`
  - `*_busy_time_pct`, `*_idle_time_pct` (가능 시)

경보(`WARN`):
- 특정 오퍼레이터 `backpressured_time_pct`가 20% 이상으로 장시간 지속

### 3.10 JVM / 컨테이너 리소스
목적:
- GC/메모리/CPU로 인한 처리율 저하 및 OOM 위험 조기 감지

권장 로그:
- 주기(`INFO`, 10초)
  - `heap_used_pct`
  - `gc_pause_ms_p95`
  - `container_memory_used_pct`
  - `container_cpu_used_pct`
  - `oom_kill_detected` (이벤트성)

경보(`WARN`/`ERROR`):
- `heap_used_pct` 고수준 지속 + `gc_pause_ms_p95` 동반 상승
- OOM kill 감지 시 즉시 `ERROR`

### 3.11 DLQ 파일 건강도
목적:
- DLQ 자체가 기록 경로/권한/용량 문제 없이 지속 가능한지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `dlq_path_mode` (`primary=/opt/flink/data` or `fallback=/tmp`)
  - `dlq_file_size_bytes`
  - `dlq_write_error_delta_10s`
  - `dlq_rotation_count` (rotation 적용 시)

경보(`WARN`/`ERROR`):
- `dlq_path_mode=fallback` 장기 지속
- `dlq_write_error_delta_10s > 0`

---

## 4) 성능 관점 메모
- 질문처럼 이 로깅은 성능에 영향이 있을 수 있다.
- 특히 워터마크/late를 이벤트 단위로 과도하게 찍으면 CPU/IO 부담이 커진다.
- 운영 기본:
  1. 10초 주기 집계 로그
  2. 경보 임계치 초과 시 `WARN`
  3. 이벤트 상세는 샘플링 `DEBUG` (또는 최초 N건/매 M건)

---

## 5) 권장 출력 예시
```text
[FLK_METRICS] ts=... win=10s src_in=125000 eps=12500 wm_in=1739988000000 wm_lag_ms=2400 reorder_buf=18200 active_keys=4300 late_delta=210 late_ratio=0.17% dlq_delta=210 dlq_deser=30 dlq_valid=40 dlq_late=140 jdbc_events_rows=118500 jdbc_pred_rows=320 retry_delta=2
```

```text
[FLK_WARN] type=late_ratio_spike win=10s late_ratio=4.8% reason_top=WATERMARK_LATE action=check_source_skew_and_watermark_progress
```

```text
[FLK_ERROR] type=dlq_write_failed path=/opt/flink/data/dead_letter_queue-... error=IOException message=\"Permission denied\"
```

---

## 6) 운영 체크리스트
- `dlq_written_delta_10s`가 갑자기 증가하는가?
- `dlq_by_reason` 상위 원인이 예상과 일치하는가?
- `consumer_lag_total`이 회복되는가?
- `watermark_lag_ms`가 장시간 악화되는가?
- `late_ratio`가 임계치 이하로 유지되는가?
- `reorder_buffer_size_estimate`가 안정적으로 수렴하는가?
- `jdbc_rows_out_delta_10s`와 source 입력 추이가 대체로 동행하는가?
- `last_completed_checkpoint_age_sec`가 정상 범위인가?
- 특정 operator `backpressured_time_pct`가 지속적으로 높은가?
- `heap_used_pct`/`gc_pause_ms_p95`가 함께 악화되는가?
- `dlq_path_mode`가 fallback으로 고착되지 않았는가?

---

## 7) 임계치 초안 (운영 시작값)
- `last_completed_checkpoint_age_sec > 2 * checkpoint_interval_sec` 3회 연속 -> `WARN`
- `operator_backpressured_time_pct > 20%` 5분 지속 -> `WARN`
- `dlq_ratio = dlq_delta / source_records_in_delta_10s > 1%` 10분 지속 -> `WARN`
- `dlq_write_error_delta_10s > 0` 즉시 -> `ERROR`
- 동일 `last_failure_exception_class + stage` 재시작 3회/10분 -> `ERROR`
