# ClickHouse Logging Guide

## 1) 목적
이 문서는 ClickHouse를 운영 관점에서 점검하기 위한 로깅/모니터링 기준이다.

핵심 목표:
- Flink JDBC sink 유입이 정상적으로 적재되는지 확인
- MergeTree 파트/머지 백로그로 인한 병목을 조기 감지
- raw/serving(`*_latest`) dedup 품질을 지속 점검
- 쿼리 지연(특히 `FINAL` 조회 비용)과 리소스 사용량을 추적
- 스키마/MV 드리프트를 빠르게 발견

---

## 2) 기본 원칙
- `INFO`: 10초~60초 주기 요약 상태
- `WARN`: 백로그 증가, 지연 악화, 중복 품질 저하 조짐
- `ERROR`: insert 실패 지속, mutation 실패 지속, 디스크 임계
- `DEBUG`: 쿼리 단건 상세는 샘플링만 허용

권장:
- payload 자체는 로깅하지 않고 집계/통계 중심으로 본다.
- "적재량 + 저장엔진 상태 + 조회 품질 + 리소스" 4축을 함께 본다.

---

## 3) 로깅 항목

### 3.1 Flink 유입 적재량 / 실패
목적:
- Flink sink 출력과 ClickHouse 입력이 동행하는지 확인

권장 로그:
- 주기(`INFO`, 10초)
  - `raw_events_rows_delta_10s` (`default.taxi_events`)
  - `raw_predictions_rows_delta_10s` (`default.taxi_predictions`)
  - `insert_query_count_delta_10s`
  - `insert_error_delta_10s`
- 이벤트성
  - insert exception: `ERROR`

경보(`WARN`):
- Flink 입력이 있는데 `raw_*_rows_delta_10s`가 장시간 0
- `insert_error_delta_10s` 증가

### 3.2 MergeTree 파트 상태 (핵심)
목적:
- 파트 과증가로 인한 insert 지연/거절 위험 조기 감지

권장 로그:
- 주기(`INFO`, 30초)
  - `active_parts_total` (`system.parts`)
  - `parts_by_table` (`taxi_events`, `taxi_predictions`, serving 테이블)
  - `parts_per_partition_max`
  - `merges_in_progress` (`system.merges`)
  - `merges_pending_estimate`

경보(`WARN`/`ERROR`):
- `active_parts_total` 급증 및 장시간 미회복
- 설정 임계 접근:
  - `parts_to_delay_insert=2000` 부근 -> `WARN`
  - `parts_to_throw_insert=4000` 부근 -> `ERROR`

참고:
- 현재 `infra/clickhouse/config.xml`에서 merge_tree 보호 임계가 설정되어 있다.

### 3.3 Mutation / DDL 백로그
목적:
- 스키마 반영/정리 작업이 누적되어 성능에 악영향 주는지 확인

권장 로그:
- 주기(`INFO`, 60초)
  - `mutations_pending` (`system.mutations`)
  - `mutations_failed`
  - `oldest_mutation_age_sec`
  - `ddl_queue_delay_sec` (클러스터 환경 시)

경보(`WARN`):
- `mutations_pending`이 지속 증가
- 실패 mutation 반복

### 3.4 raw vs serving dedup 품질
목적:
- at-least-once raw 중복이 serving 뷰에서 정상 완화되는지 확인

권장 로그:
- 주기(`INFO`, 60초)
  - `raw_dup_groups_events`
  - `raw_dup_groups_predictions`
  - `latest_dup_groups_events` (기대값: 0)
  - `latest_dup_groups_predictions` (기대값: 0)

경보(`WARN`):
- `latest_dup_groups_* > 0`
- raw duplicate 증가율 급상승

### 3.5 조회 지연 / FINAL 비용
목적:
- 대시보드/운영 조회 품질 악화 조기 감지

권장 로그:
- 주기(`INFO`, 30초~60초)
  - `query_latency_ms_p50/p95` (`taxi_events_latest`, `taxi_predictions_latest`)
  - `read_rows_delta`, `read_bytes_delta` (`system.query_log`)
  - `final_query_ratio` (`FINAL` 사용 조회 비율)

경보(`WARN`):
- `p95` 지연이 기준 초과로 지속
- `read_rows` 급증 대비 결과 row 낮은 패턴 반복

### 3.6 리소스 상태 (디스크/메모리/CPU)
목적:
- 저장소/메모리 병목과 장애 위험 조기 탐지

권장 로그:
- 주기(`INFO`, 30초)
  - `disk_used_pct`, `disk_free_bytes` (`system.disks`)
  - `memory_usage_bytes`
  - `cpu_usage_pct` (컨테이너 메트릭 연계)
  - `uncompressed_cache_hit_ratio` (가능 시)

경보(`WARN`/`ERROR`):
- 디스크 사용률 임계 접근
- 메모리 사용량 고수준 + query latency 동반 악화

### 3.7 스키마/MV 드리프트 점검
목적:
- 배포/재기동 후 객체 누락으로 파이프라인 단절되는 상황 방지

권장 로그:
- 시작 1회(`INFO`)
  - 필수 객체 존재 여부:
    - `taxi_events`, `taxi_predictions`
    - `taxi_events_serving`, `taxi_predictions_serving`
    - `mv_taxi_events_to_serving`, `mv_taxi_predictions_to_serving`
    - `taxi_events_latest`, `taxi_predictions_latest`
- 주기(`INFO`, 10분)
  - `schema_drift_detected` (bool)

경보(`ERROR`):
- 필수 MV/VIEW 누락

---

## 4) 성능 관점 메모
- ClickHouse는 과도한 상세 로깅보다 `system.*` 기반 집계가 안정적이다.
- `query_log` 전수 분석은 비용이 크므로 운영 기본은 집계/샘플링으로 제한한다.
- `FINAL` 조회가 많은 경우 p95 지연과 read_rows를 함께 본다.
- 파트 수 급증은 insert 지연/거절로 직결되므로 최우선 지표로 둔다.

---

## 5) 권장 출력 예시
```text
[CH_METRICS] ts=... win=30s ins_events=118500 ins_pred=320 insert_err=0 active_parts=742 merges_run=5 mut_pending=0 raw_dup_evt=159 latest_dup_evt=0 q_p95_latest_ms=180 disk_used_pct=61.2
```

```text
[CH_WARN] type=parts_growth_spike table=default.taxi_events active_parts=1850 threshold_delay=2000 action=check_batch_size_and_merge_backlog
```

```text
[CH_ERROR] type=insert_rejected reason=too_many_parts table=default.taxi_events parts=4012 threshold_throw=4000
```

---

## 6) 운영 체크리스트
- `raw_*_rows_delta`가 Flink 출력 추이와 동행하는가?
- `active_parts_total`이 지속 증가 후 회복되지 않는가?
- `merges_in_progress`/`mutations_pending`가 비정상적으로 누적되는가?
- `latest_dup_groups_*`가 0을 유지하는가?
- `taxi_*_latest` 조회 p95가 허용 범위 내인가?
- 디스크/메모리 사용률이 임계 접근 중인가?
- 필수 MV/VIEW 객체가 누락되지 않았는가?

---

## 7) 임계치 초안 (운영 시작값)
- `active_parts_total >= 1500` 5분 지속 -> `WARN`
- `active_parts_total >= 3500` 즉시 -> `ERROR`
- `latest_dup_groups_* > 0` 2회 연속 -> `WARN`
- `insert_error_delta_10s > 0` 3회 연속 -> `ERROR`
- `q_p95_latest_ms`가 기준(예: 500ms) 초과 10분 지속 -> `WARN`
- `disk_used_pct >= 80%` -> `WARN`, `>= 90%` -> `ERROR`
