# Pipeline Optimization Changelog & Verification

작성일: 2026-02-14 (UTC 기준)

## 1. 목적

이 문서는 이번 세션에서 적용한 변경사항을 최종 기준으로 정리하고, 실제로 동작 검증한 결과를 남깁니다.

## 2. 변경 파일 (최종)

### Generator
- `services/generator/generate.cpp`
  - `SIGPIPE` 무시 처리 추가
  - sender 스레드 수 상한 적용
  - queue timed pop(`try_pop_for`) 적용

### Nginx
- `infra/nginx/templates/nginx.distributed.conf.template`
- `infra/nginx/nginx.single-machine.conf`
  - `least_conn + keepalive` 동작 보강
  - upstream failover 옵션 추가
  - proxy timeout/buffer 튜닝

### Ingestor
- `services/ingestor/docker-compose.yml`
  - buffer/batch/concurrency 튜닝
- `services/ingestor/src/main/resources/application.yml`
  - Kafka producer 배치/버퍼 튜닝

### Flink
- `infra/flink/docker-compose.yml`
  - parallelism/slots/jdbc batch 파라미터 상향
- `services/flink-job/pom.xml`
  - `httpclient5` 의존성 추가 (ClickHouse JDBC runtime class 이슈 보완)

### ClickHouse
- `infra/clickhouse/schema.sql`
  - `PARTITION BY toYYYYMM(ts)`
  - `ORDER BY (ts, zone_id, trip_id)`

### Grafana / GeoJSON
- `infra/grafana/provisioning/dashboards/taxi-events.json`
  - 불필요 패널 3개 제거 (id: 8, 10, 11)
  - Geomap 패널을 `% 점유율` 기준 표기로 변경
- `infra/grafana/scripts/update_geojson.py`
  - no-cache HTTP 서빙 추가
  - tier 기준을 절대값 -> `% 점유율`로 변경
  - 집계 윈도우 env화 (`LOOKBACK_MINUTES`)
- `infra/grafana/docker-compose.yml`
  - `UPDATE_INTERVAL=900` (15분)
  - `LOOKBACK_MINUTES=1440` (24시간)

### 문서
- `README.md`
  - 문서 우선순위 섹션을 `docs/` 기준으로 업데이트
- `docs/runtime-runbook.md`
  - env 추적 정책 반영
  - Geomap 운영값(갱신주기/윈도우/해석) 추가

## 3. 불필요 변경 정리 결과

- 롤백 완료
  - `README.md` 과거 임시 대규모 변경 이력은 정리 후 현재 내용으로 재반영
  - `config/.env.distributed`, `config/.env.single-machine`는 git 추적 해제 정책에 맞춰 인덱스에서 제거
  - `services/generator/config/default.yaml` 재생 속도 기본값은 `5x`로 유지
- 추적 제외 정책
  - `.gitignore`에 `*.env`, `**/.env*` 패턴 적용
- 커밋 제외 권장(로컬 산출물)
  - `.run/`, `generator/` (로컬 실행 산출물)

## 4. 동작 검증 결과

### 4.1 Nginx

검증 시점 로그 기준:
- 최근 20초 응답: `202`만 확인 (`10862`건)
- 업스트림 분산:
  - `172.18.0.9:8080` -> `3711`
  - `172.18.0.7:8080` -> `3595`
  - `172.18.0.8:8080` -> `3556`

결론: Nginx 편향 없이 정상 분산.

### 4.2 Flink

- Job 상태: `RUNNING`
- 병렬도: `4`
- ClickHouse sink class 누락 이슈(`NoClassDefFoundError`)는 의존성 반영 후 재기동으로 완화.

### 4.3 ClickHouse 적재율

측정 창(10초):
- before: `31,668,560`
- after: `32,096,646`
- rate: 약 `42,808 rows/s`

### 4.4 Geomap 로직 검증

핵심 확인:
- 전체 적재 행수: `53,475,613`
- 최근 24시간 활성 존 수
  - 전체 이벤트 기준: `258/260`
  - `PICKUP` 기준: `239/260`

따라서 회색(`No Data (0%)`)은 데이터 유실이 아니라,
"해당 존의 최근 24시간 `PICKUP`이 0"임을 의미.

검증 예시:
- `zone_id=251` (Staten Island)
  - 최근 24시간 전체 이벤트: `97`
  - 최근 24시간 `PICKUP`: `0`

## 5. 현재 Geomap 운영 정책

- 갱신 주기: 15분 (`UPDATE_INTERVAL=900`)
- 집계 윈도우: 최근 24시간 (`LOOKBACK_MINUTES=1440`)
- 지표 기준: `PICKUP` 점유율 (`demand_pct`)

필요 시 정책 변경:
- 회색 영역을 줄이려면 `PICKUP` 대신 `ALL EVENTS` 또는 `PICKUP + DROPOFF` 기준으로 변경 가능.
