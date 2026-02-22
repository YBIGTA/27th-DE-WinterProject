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

### 3.8 데이터 최신성(Freshness) 지표 (추가)
목적:
- "데이터가 들어오고는 있으나 조회는 과거 상태"인 상황을 조기 감지

권장 로그:
- 주기(`INFO`, 30초)
  - `raw_latest_event_age_sec = now() - max(raw.event_time)`
  - `serving_latest_event_age_sec = now() - max(serving.event_time)`
  - `ingest_to_serving_lag_sec = max(0, max(raw.event_time) - max(serving.event_time))`

경보(`WARN`):
- `raw_*_rows_delta_10s > 0` 상태에서 `serving_latest_event_age_sec` 장시간 증가
- `ingest_to_serving_lag_sec`가 기준치(예: 60~120초) 초과로 지속

### 3.9 Insert 경로 병목 분해 (추가)
목적:
- insert 오류뿐 아니라 "느려짐/비효율"까지 분리 관찰

권장 로그:
- 주기(`INFO`, 10초~30초, `system.query_log`)
  - `insert_latency_ms_p95`
  - `rows_per_insert_avg`
  - `insert_bytes_per_sec`
  - `insert_error_by_code_topk`

경보(`WARN`/`ERROR`):
- `insert_latency_ms_p95` 상승 + `raw_*_rows_delta_10s` 정체
- 특정 error code 반복(예: too many parts, timeout, network)

### 3.10 Merge 해소 속도 지표 (추가)
목적:
- "머지가 돌아가고는 있는지"가 아니라 "백로그를 실제 해소 중인지" 확인

권장 로그:
- 주기(`INFO`, 30초)
  - `merge_rows_per_sec = delta(system.events[MergedRows]) / window`
  - `merge_bytes_per_sec = delta(system.events[MergedUncompressedBytes]) / window`
  - `oldest_active_part_age_sec`

경보(`WARN`):
- `active_parts_total` 고수준 + `merge_rows_per_sec` 저수준이 동시 지속
- `oldest_active_part_age_sec`가 증가만 하고 회복되지 않음

### 3.11 쿼리 클래스별 성능 분리 (추가)
목적:
- 대시보드/API/배치 중 어느 경로가 지연을 유발하는지 즉시 식별

권장 로그:
- 주기(`INFO`, 60초, `system.query_log`)
  - `query_class_latency_ms_p95` (class별)
  - `query_class_error_rate`
  - `read_amp_ratio = read_rows / result_rows`
  - `final_query_ratio_by_class`

구현 메모:
- 애플리케이션 조회 전 `SET log_comment='class=dashboard'` 형태로 class 태깅 권장
- class 미설정 쿼리는 `unknown`으로 집계

### 3.12 디스크 추세/포화 예측 (추가)
목적:
- 현재 사용률뿐 아니라 "언제 꽉 차는지"를 운영자가 미리 파악

권장 로그:
- 주기(`INFO`, 1분)
  - `disk_growth_bytes_1h`
  - `days_to_full`

경보(`WARN`/`ERROR`):
- `disk_used_pct`가 낮아도 `days_to_full`이 짧으면 조기 `WARN` (예: < 7일)
- `days_to_full < 3일` 또는 `disk_used_pct >= 90%`면 `ERROR`

### 3.13 무트래픽 가드(Guard) 조건 (추가)
목적:
- 야간/무부하 구간에서 불필요한 오탐을 줄이고 경보 품질 유지

권장 적용:
- 지연/에러/중복 경보는 아래 guard 충족 시에만 평가
  - `insert_query_count_delta_10s >= 1` 또는
  - `raw_events_rows_delta_10s + raw_predictions_rows_delta_10s > 0`

예시:
- `q_p95_latest_ms` 경보는 "최근 5분 평균 insert > 0"일 때만 유효
- `latest_dup_groups_*` 경보는 "최근 5분 raw 유입 > 0"일 때만 유효

---

## 4) 성능 관점 메모
- ClickHouse는 과도한 상세 로깅보다 `system.*` 기반 집계가 안정적이다.
- `query_log` 전수 분석은 비용이 크므로 운영 기본은 집계/샘플링으로 제한한다.
- `FINAL` 조회가 많은 경우 p95 지연과 read_rows를 함께 본다.
- 파트 수 급증은 insert 지연/거절로 직결되므로 최우선 지표로 둔다.
- `query_log` 기반 지표를 쓰려면 로그 보존 기간/flush 주기를 운영 기준으로 사전 확정한다.
- freshness는 반드시 `event_time` 기준과 시스템 적재시각 기준(가능 시) 둘 다 검토한다.

---

## 5) 권장 출력 예시
```text
[CH_METRICS] ts=... win=30s ins_events=118500 ins_pred=320 insert_err=0 insert_p95_ms=85 rows_per_ins=950 active_parts=742 merge_rows_s=125000 mut_pending=0 raw_dup_evt=159 latest_dup_evt=0 serving_age_s=12 ingest_to_serving_lag_s=8 q_p95_latest_ms=180 read_amp=32.5 disk_used_pct=61.2 days_to_full=18.4
```

```text
[CH_WARN] type=parts_growth_spike table=default.taxi_events active_parts=1850 threshold_delay=2000 action=check_batch_size_and_merge_backlog
```

```text
[CH_ERROR] type=insert_rejected reason=too_many_parts table=default.taxi_events parts=4012 threshold_throw=4000
```

```text
[CH_WARN] type=freshness_lag table=default.taxi_events_latest serving_age_s=145 raw_age_s=11 lag_s=134 guard_insert_qps=on action=check_mv_and_merge_backlog
```

---

## 6) 운영 체크리스트
- `raw_*_rows_delta`가 Flink 출력 추이와 동행하는가?
- `active_parts_total`이 지속 증가 후 회복되지 않는가?
- `merge_rows_per_sec`가 파트 증가 속도를 따라잡는가?
- `merges_in_progress`/`mutations_pending`가 비정상적으로 누적되는가?
- `latest_dup_groups_*`가 0을 유지하는가?
- `serving_latest_event_age_sec`와 `ingest_to_serving_lag_sec`가 안정적인가?
- `taxi_*_latest` 조회 p95가 허용 범위 내인가?
- `read_amp_ratio`가 비정상 고수준으로 고착되지 않았는가?
- 디스크/메모리 사용률이 임계 접근 중인가?
- `disk_growth_bytes_1h` 기준 `days_to_full`이 위험 수준인가?
- 필수 MV/VIEW 객체가 누락되지 않았는가?

---

## 7) 임계치 초안 (운영 시작값)
- `active_parts_total >= 1500` 5분 지속 -> `WARN` (`guard: insert_query_count_delta_10s > 0`)
- `active_parts_total >= 3500` 즉시 -> `ERROR`
- `merge_rows_per_sec` 저수준 + `active_parts_total` 상승 10분 지속 -> `WARN`
- `latest_dup_groups_* > 0` 2회 연속 -> `WARN` (`guard: 최근 5분 raw 유입 > 0`)
- `insert_error_delta_10s > 0` 3회 연속 -> `ERROR`
- `insert_latency_ms_p95 > 1000ms` 10분 지속 -> `WARN` (`guard: insert_query_count_delta_10s >= 5`)
- `serving_latest_event_age_sec > 120` 10분 지속 -> `WARN` (`guard: raw_rows_delta_10s > 0`)
- `ingest_to_serving_lag_sec > 60` 10분 지속 -> `WARN`
- `q_p95_latest_ms`가 기준(예: 500ms) 초과 10분 지속 -> `WARN` (`guard: select_count_delta_1m > 0`)
- `read_amp_ratio > 100` 10분 지속 -> `WARN`
- `disk_used_pct >= 80%` -> `WARN`, `>= 90%` -> `ERROR`
- `days_to_full < 7` -> `WARN`, `< 3` -> `ERROR`

---

## 8) 구현 SQL 템플릿 (운영 쿼리)

### 8.1 Insert 성공/실패/지연 (`system.query_log`)
```sql
SELECT
  countIf(exception_code = 0) AS insert_ok_10s,
  countIf(exception_code != 0) AS insert_err_10s,
  quantileTDigestIf(0.95)(query_duration_ms, exception_code = 0) AS insert_latency_ms_p95,
  avgIf(written_rows, exception_code = 0) AS rows_per_insert_avg,
  sumIf(written_bytes, exception_code = 0) / 10.0 AS insert_bytes_per_sec
FROM system.query_log
WHERE event_time >= now() - INTERVAL 10 SECOND
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND hasAny(tables, ['taxi_events', 'taxi_predictions']);
```

### 8.2 조회 지연/FINAL/read amplification (`system.query_log`)
```sql
SELECT
  quantileTDigest(0.50)(query_duration_ms) AS q_p50_ms,
  quantileTDigest(0.95)(query_duration_ms) AS q_p95_ms,
  round(countIf(positionCaseInsensitive(query, 'FINAL') > 0) / nullIf(count(), 0), 4) AS final_query_ratio,
  sum(read_rows) AS read_rows,
  sum(result_rows) AS result_rows,
  round(sum(read_rows) / nullIf(sum(result_rows), 0), 2) AS read_amp_ratio
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 MINUTE
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND hasAny(tables, ['taxi_events_latest', 'taxi_predictions_latest']);
```

### 8.3 Freshness (`event_time` 기준)
```sql
SELECT dateDiff('second', max(event_time), now()) AS raw_latest_event_age_sec
FROM default.taxi_events;
```

```sql
SELECT dateDiff('second', max(event_time), now()) AS serving_latest_event_age_sec
FROM default.taxi_events_latest;
```

```sql
WITH
  (SELECT max(event_time) FROM default.taxi_events) AS raw_max_ts,
  (SELECT max(event_time) FROM default.taxi_events_latest) AS serving_max_ts
SELECT greatest(0, dateDiff('second', serving_max_ts, raw_max_ts)) AS ingest_to_serving_lag_sec;
```

### 8.4 Part/merge 상태 (`system.parts`, `system.merges`, `system.events`)
```sql
SELECT
  count() AS active_parts_total,
  max(parts_per_partition) AS parts_per_partition_max
FROM
(
  SELECT
    table,
    partition_id,
    count() AS parts_per_partition
  FROM system.parts
  WHERE database = 'default'
    AND active
    AND table IN ('taxi_events', 'taxi_predictions', 'taxi_events_serving', 'taxi_predictions_serving')
  GROUP BY table, partition_id
);
```

```sql
SELECT
  count() AS merges_in_progress,
  sum(total_size_bytes_compressed) AS merges_bytes_in_progress
FROM system.merges
WHERE database = 'default';
```

```sql
SELECT event, value
FROM system.events
WHERE event IN ('MergedRows', 'MergedUncompressedBytes');
```

### 8.5 Mutation 상태 (`system.mutations`)
```sql
SELECT
  countIf(is_done = 0) AS mutations_pending,
  countIf(is_done = 0 AND latest_failed_part != '') AS mutations_failed,
  maxIf(dateDiff('second', create_time, now()), is_done = 0) AS oldest_mutation_age_sec
FROM system.mutations
WHERE database = 'default';
```

### 8.6 디스크 사용률 (`system.disks`)
```sql
SELECT
  name,
  round(100.0 * (total_space - free_space) / nullIf(total_space, 0), 2) AS disk_used_pct,
  free_space AS disk_free_bytes
FROM system.disks;
```

### 8.7 Query class 분리 (`log_comment` 기반)
```sql
SELECT
  if(empty(log_comment), 'unknown', log_comment) AS query_class,
  quantileTDigest(0.95)(query_duration_ms) AS latency_p95_ms,
  countIf(exception_code != 0) AS error_count,
  round(sum(read_rows) / nullIf(sum(result_rows), 0), 2) AS read_amp_ratio
FROM system.query_log
WHERE event_time >= now() - INTERVAL 5 MINUTE
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
GROUP BY query_class
ORDER BY latency_p95_ms DESC;
```

운영 메모:
- `disk_growth_bytes_1h`, `days_to_full`은 시계열 관측이 필요하므로 Prometheus recording rule로 계산하는 것을 권장한다.
- `query_log` 비용이 우려되면 10~60초 집계 전용 쿼리로 제한하고, 상세 원문 탐색은 Loki로 넘긴다.

---

## 9) Prometheus/Loki 정석 매핑 (2026-02-22)

### 9.1 Prometheus 수집 경로 (active)
- ClickHouse native `/metrics` endpoint 활성화
  - 설정 파일: `infra/clickhouse/config.xml`
  - 포트/경로: `:9363/metrics`
  - compose 노출:
    - `infra/clickhouse/docker-compose.yml`
    - `infra/clickhouse/docker-compose.distributed.yml`
- Prometheus scrape:
  - single: `clickhouse:9363`
  - distributed: `${CLICKHOUSE_IP}:${CLICKHOUSE_PROMETHEUS_PORT:-9363}`
  - 파일:
    - `infra/prometheus/prometheus.yml.tmpl`
    - `infra/prometheus/prometheus.distributed.yml`

### 9.2 Prometheus 우선 관측 축
- 적재량/실패: insert rows, insert error rate
- MergeTree 상태: active parts, merge backlog, merge throughput
- Mutation 상태: pending/failed/age
- 조회 성능: query latency, read rows/bytes, FINAL usage ratio
- 리소스: disk usage/free, memory, CPU

구현 원칙:
- `/metrics`에서 바로 얻기 어려운 항목(`freshness`, `dedup`, `query_class`)은 `system.*` 집계(SQL)로 보완한다.
- SQL 집계 결과는 exporter 또는 recording rule로 승격해 Prometheus 경보와 연결한다.

### 9.3 Loki 보완 범위
- Prometheus 경보 이후 원인 확정 용도
- 필수 이벤트:
  - `too_many_parts` insert reject
  - mutation failure 반복
  - query exception/timeout
  - MV 누락/스키마 드리프트 감지 이벤트
- 필수 필드:
  - `db`, `table`, `type`, `code`, `action`

### 9.4 시작 쿼리 (예시)
- PromQL:
  - insert rows/s: `rate({__name__=~"ClickHouse(ProfileEvents|ProfileEvent)_InsertedRows.*"}[1m])`
  - active parts: `{__name__=~"ClickHouse(Metrics|Metric)_PartsActive.*"}`
- LogQL:
  - too many parts: `{service="clickhouse"} |= "too_many_parts"`
  - mutation 실패: `{service="clickhouse"} |= "mutation" |= "failed"`
