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
- ONNX/예측 경로는 상세 추적보다 "최소 헬스 로그"로 정상/장애를 빠르게 구분

---

## 2) 기본 원칙
- `INFO`: 10초 주기 요약 로그(기본)
- `WARN`: 임계치 초과(지연/late ratio/재시도 급증)
- `ERROR`: DLQ write 실패, sink flush 실패, 지속적 backlog
- `DEBUG`: 이벤트 상세는 샘플링만 허용

권장:
- 이벤트 1건당 상세 로그 상시 출력 금지
- "주기 집계 + 임계치 경보 + 샘플 상세" 조합 사용

### 2.1 공통 로그 필드 표준 (필수)
운영자가 병목 위치를 빠르게 특정할 수 있도록 아래 필드는 모든 `FLK_*` 로그에 공통 포함한다.

- `ts`: epoch millis 또는 RFC3339
- `env`: `dev|stg|prod`
- `job`: `TaxiRealtimeJob`
- `job_id`: Flink job id
- `tm_id`: taskmanager 식별자
- `operator`: source/reorder/window/onnx/jdbc 등
- `subtask`: subtask index
- `win`: 집계 윈도우 (`10s`, `60s`)
- `topic`, `partition`: source 관측 로그에서 필수
- `type`, `action`: `WARN/ERROR` 원인과 즉시 액션

### 2.2 지표 계산 정의 (운영 혼선 방지)
아래 항목은 문서 전반에서 같은 의미로 사용한다.

- `source_in_delta_10s`: Kafka source가 10초 동안 읽은 총 이벤트 수
- `main_out_delta_10s`: 검증/재정렬 후 main stream으로 배출된 이벤트 수
- `dlq_delta_10s`: 같은 구간 DLQ 기록 건수
- `filtered_drop_delta_10s`: "의도된 비즈니스 drop" 건수
- `reorder_seen_delta_10s`: reorder 오퍼레이터가 본 총 이벤트 수
- `data_balance_gap_delta_10s = source_in_delta_10s - (main_out_delta_10s + dlq_delta_10s + filtered_drop_delta_10s)`
- `data_balance_gap_ratio = data_balance_gap_delta_10s / source_in_delta_10s` (`source_in_delta_10s > 0`일 때)
- `late_ratio = late_events_delta_10s / reorder_seen_delta_10s` (`reorder_seen_delta_10s > 0`일 때)

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
- `source_in_delta_10s ~= main_out_delta_10s + dlq_delta_10s + filtered_drop_delta_10s`
- 함께 기록:
  - `data_balance_gap_delta_10s`
  - `data_balance_gap_ratio`
- 위 식 불일치가 지속되면 누락 계측 또는 경로 누수 가능성 점검

### 3.2 Kafka source 소비 속도/지연
목적:
- Flink가 실제로 Kafka를 충분히 따라가고 있는지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `source_records_in_delta_10s`
  - `source_records_per_sec`
  - `consumer_lag_total` (외부 `kafka-consumer-groups`/exporter 연계)
  - `consumer_lag_max_partition`
  - `consumer_lag_p95_partition`
  - `lagged_partition_count`
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
  - `watermark_stall_sec` (watermark 무진행 누적 시간)
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
  - `checkpoint_alignment_ms_p50/p95`
  - `checkpoint_sync_duration_ms_p50/p95`
  - `checkpoint_async_duration_ms_p50/p95`
  - `checkpoint_state_size_bytes_p50/p95`

경보(`WARN`):
- `last_completed_checkpoint_age_sec`가 체크포인트 주기의 2배 이상으로 지속
- `checkpoint_failed_delta_10s`가 연속 증가
- `checkpoint_alignment_ms_p95` 급증 + backpressure 동반

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
  - `operator_backpressured_time_pct{operator}`
  - `operator_busy_time_pct{operator}`
  - `operator_idle_time_pct{operator}`
  - `operator_backpressured_time_pct{operator,subtask}` (상위 N개 subtask)
  - `operator_records_in_delta_10s{operator,subtask}`
  - `operator_subtask_skew_ratio = max(records_in_subtask) / median(records_in_subtask)`

경보(`WARN`):
- 특정 오퍼레이터 `backpressured_time_pct`가 20% 이상으로 장시간 지속
- `operator_subtask_skew_ratio`가 장시간 상승 (핫키/분배 불균형 의심)

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

### 3.12 ML 최소 헬스 로그 세트 (운영 기본)
목적:
- 예측 경로를 "느낌"이 아니라 최소 신호로 정상/장애 구분

권장 로그(최소 4개, `INFO`, 10초):
- `demand_rows_in_delta_10s`
- `prediction_rows_out_delta_10s`
- `onnx_infer_error_delta_10s`
- `lag20_ready_zone_count`

운영 기준(최소 2개):
- `demand_rows_in_delta_10s > 0`인데 `prediction_rows_out_delta_10s = 0`가 20분 이상 지속 -> `WARN`
- `onnx_infer_error_delta_10s > 0` -> 즉시 `ERROR`

메모:
- 상세 feature/모델 입력값 로깅은 기본 비활성.
- 원인 분석이 필요할 때만 샘플링 `DEBUG`를 일시 활성화.

### 3.13 End-to-End 지연 (핵심 성능 지표)
목적:
- "파이프라인이 느리다"를 정량으로 판별하고 구간별 원인을 분리

권장 로그:
- 주기(`INFO`, 10초)
  - `e2e_latency_ms_p50/p95/p99` (기준: `sink_commit_ts - event_ts`)
  - `e2e_slo_violation_delta_10s`
  - `source_to_reorder_latency_ms_p95`
  - `reorder_to_jdbc_latency_ms_p95`
  - `demand_window_wait_ms_p95` (예측 경로)
- 이벤트성(`WARN`)
  - `e2e_latency_spike` (최근 p95 급등)

경보(`WARN`/`ERROR`):
- `e2e_latency_ms_p95`가 SLO 초과로 지속
- `e2e_slo_violation_delta_10s`가 연속 증가

### 3.14 파티션/키 스큐 탐지
목적:
- "전체 평균은 정상인데 일부 subtask만 막히는" 병목을 조기 탐지

권장 로그:
- 주기(`INFO`, 30초)
  - `source_partition_lag_topk` (partition, lag)
  - `hot_trip_key_topk_sampled` (key hash, count)
  - `operator_subtask_skew_ratio{operator}`
  - `jdbc_inflight_batches{subtask}`
- 이벤트성(`WARN`)
  - `partition_skew_detected`
  - `hot_key_skew_detected`

---

## 4) 성능 관점 메모
- 질문처럼 이 로깅은 성능에 영향이 있을 수 있다.
- 특히 워터마크/late를 이벤트 단위로 과도하게 찍으면 CPU/IO 부담이 커진다.
- subtask/partition 라벨은 cardinality가 커질 수 있어 "상위 N개 + 30초 주기"로 제한한다.
- 운영 기본:
  1. 10초 주기 집계 로그
  2. 경보 임계치 초과 시 `WARN`
  3. 이벤트 상세는 샘플링 `DEBUG` (또는 최초 N건/매 M건)

---

## 5) 권장 출력 예시
```text
[FLK_METRICS] ts=... env=prod job=TaxiRealtimeJob job_id=... tm_id=tm-2 operator=reorder subtask=3 win=10s src_in=125000 eps=12500 lag_total=18000 lag_p95_part=310 wm_in=1739988000000 wm_lag_ms=2400 wm_stall_sec=0 reorder_buf=18200 active_keys=4300 late_delta=210 late_ratio=0.17% dlq_delta=210 balance_gap=70 gap_ratio=0.056% jdbc_events_rows=118500 jdbc_pred_rows=320 retry_delta=2 e2e_p95_ms=12800
```

```text
[FLK_WARN] ts=... env=prod job_id=... type=late_ratio_spike operator=reorder subtask=7 win=10s late_ratio=4.8% reason_top=WATERMARK_LATE action=check_source_skew_and_watermark_progress
```

```text
[FLK_ERROR] ts=... env=prod job_id=... type=dlq_write_failed operator=dlq_writer path=/opt/flink/data/dead_letter_queue-... error=IOException message=\"Permission denied\" action=failover_to_tmp_and_raise_p1
```

---

## 6) 운영 체크리스트
- `dlq_written_delta_10s`가 갑자기 증가하는가?
- `dlq_by_reason` 상위 원인이 예상과 일치하는가?
- `consumer_lag_total`이 회복되는가?
- `consumer_lag_max_partition` 상위 파티션이 고착되는가?
- `watermark_lag_ms`가 장시간 악화되는가?
- `watermark_stall_sec`가 증가하는가?
- `late_ratio`가 임계치 이하로 유지되는가?
- `reorder_buffer_size_estimate`가 안정적으로 수렴하는가?
- `jdbc_rows_out_delta_10s`와 source 입력 추이가 대체로 동행하는가?
- `jdbc_flush_latency_ms_p95`가 상승한 상태로 유지되는가?
- `last_completed_checkpoint_age_sec`가 정상 범위인가?
- `checkpoint_alignment_ms_p95`/`checkpoint_state_size_bytes_p95`가 함께 상승하는가?
- 특정 operator `backpressured_time_pct`가 지속적으로 높은가?
- `operator_subtask_skew_ratio`가 높아 일부 subtask만 과부하인가?
- `heap_used_pct`/`gc_pause_ms_p95`가 함께 악화되는가?
- `dlq_path_mode`가 fallback으로 고착되지 않았는가?
- `demand_rows_in_delta_10s > 0`인데 `prediction_rows_out_delta_10s = 0` 상태가 장시간 지속되는가?
- `onnx_infer_error_delta_10s`가 0을 유지하는가?
- `e2e_latency_ms_p95/p99`가 SLO 내로 회복되는가?

---

## 7) 임계치 초안 (운영 시작값)
- `last_completed_checkpoint_age_sec > 2 * checkpoint_interval_sec` 3회 연속 -> `WARN`
- `operator_backpressured_time_pct > 20%` 5분 지속 -> `WARN`
- `operator_subtask_skew_ratio > 2.5` 10분 지속 -> `WARN`
- `dlq_ratio = dlq_delta / source_records_in_delta_10s > 1%` 10분 지속 -> `WARN`
- `data_balance_gap_ratio > 0.5%` 5분 지속 -> `WARN`, `> 2%` 즉시 -> `ERROR`
- `consumer_lag_total` 10분 연속 증가 + `source_records_per_sec` 저하 -> `WARN`
- `watermark_stall_sec > 120` (`source_records_in_delta_10s > 0`) -> `WARN`
- `watermark_lag_ms > 30000` 10분 지속 -> `WARN`
- `jdbc_flush_latency_ms_p95 > 5000` 10분 지속 -> `WARN`
- `checkpoint_alignment_ms_p95 > 10000` 5분 지속 -> `WARN`
- `dlq_write_error_delta_10s > 0` 즉시 -> `ERROR`
- 동일 `last_failure_exception_class + stage` 재시작 3회/10분 -> `ERROR`
- `demand_rows_in_delta_10s > 0` + `prediction_rows_out_delta_10s = 0` 20분 지속 -> `WARN`
- `onnx_infer_error_delta_10s > 0` 즉시 -> `ERROR`
- `e2e_latency_ms_p95 > 60000` 10분 지속 -> `WARN`
- `e2e_latency_ms_p99 > 120000` 5분 지속 -> `ERROR`

---

## 8) Prometheus/Loki 정석 매핑 (2026-02-22)

### 8.1 Prometheus 수집 경로 (active)
- Flink 공식 PrometheusReporter 사용
  - 설정 파일:
    - `infra/flink/docker-compose.yml`
    - `infra/flink/docker-compose.distributed.yml`
  - reporter 설정:
    - `metrics.reporters: prom`
    - `metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory`
    - `metrics.reporter.prom.port: 9249`
- 스크랩 타겟:
  - single: `flink-jobmanager:9249`, `flink-taskmanager-{1..3}:9249`
  - distributed:
    - `${FLINK_IP}:${FLINK_JOBMANAGER_METRICS_PORT:-9249}`
    - `${FLINK_IP}:${FLINK_TASKMANAGER_1_METRICS_PORT:-9250}`
    - `${FLINK_IP}:${FLINK_TASKMANAGER_2_METRICS_PORT:-9251}`
    - `${FLINK_IP}:${FLINK_TASKMANAGER_3_METRICS_PORT:-9252}`
  - 파일:
    - `infra/prometheus/prometheus.yml.tmpl`
    - `infra/prometheus/prometheus.distributed.yml`

### 8.2 Prometheus 우선 관측 축 (지표 의미 기준)
- Source 처리량: `numRecordsIn/numRecordsOut` 계열
- 워터마크/지연: `currentInputWatermark/currentOutputWatermark` 계열
- 백프레셔: `backPressuredTimeMsPerSecond`, `busyTimeMsPerSecond`, `idleTimeMsPerSecond` 계열
- 체크포인트: completed/failed count, last checkpoint duration/size/alignment 계열
- 실패/재시작: job restart/failure count 계열

운영 원칙:
- metric 이름은 Flink 버전에 따라 suffix가 달라질 수 있으므로, 최초 적용 시 `/metrics` raw를 확인하고 매핑표를 고정한다.
- 경보는 metric 이름이 아니라 의미 축(`checkpoint_age`, `backpressure`, `late_ratio`) 기준으로 유지한다.

### 8.3 Loki 보완 범위
- Prometheus가 이상을 탐지하면 아래 로그로 원인을 확정한다.
- 필수 이벤트:
  - deserialization/validation 실패
  - `LATE_EVENTS` 사유(`WATERMARK_LATE`, `REORDER_LATE`)
  - JDBC sink flush/retry/error
  - DLQ write 실패
  - ONNX inference error
- 필수 필드:
  - `job_id`, `operator`, `subtask`, `type`, `action`

### 8.4 시작 쿼리 (예시)
- PromQL:
  - 총 입력 처리량: `sum(rate({__name__=~"flink_.*numRecordsIn.*"}[1m]))`
  - 총 출력 처리량: `sum(rate({__name__=~"flink_.*numRecordsOut.*"}[1m]))`
  - 백프레셔 상위 오퍼레이터: `topk(5, max by (job_name,task_name,subtask_index) ({__name__=~"flink_.*backPressuredTimeMsPerSecond.*"}))`
- LogQL:
  - late 원인: `{service="flink"} |= "LATE_EVENTS" |~ "WATERMARK_LATE|REORDER_LATE"`
  - sink 오류: `{service="flink"} |= "jdbc" |= "ERROR"`
