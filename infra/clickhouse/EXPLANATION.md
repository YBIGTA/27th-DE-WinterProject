---
component: clickhouse-docker-compose
status: CURRENT
last_reviewed: 2026-02-22
core_files:
  - infra/clickhouse/docker-compose.yml
  - infra/clickhouse/docker-compose.distributed.yml
  - infra/clickhouse/schema.sql
  - infra/clickhouse/scripts/apply_schema.sh
---

# clickhouse-docker-compose

## Role
로컬/개발 환경에서 ClickHouse 서버를 Docker Compose로 구동하고 raw 적재 테이블 + dedup serving 레이어(`*_latest`)를 함께 관리한다.

## I/O Flow
```
[Flink JDBC Sink] --> [ClickHouse Raw Tables]
[ClickHouse Raw Tables] --(MV + FINAL)--> [Serving Views (*_latest)]
[Client Apps/Grafana] --(HTTP 8123 / TCP 9000)--> [ClickHouse Container] --(Filesystem)--> [clickhouse_data Volume]
```

## Core behavior
1. ClickHouse 컨테이너(`clickhouse/clickhouse-server:23.8`) 실행
2. HTTP(8123), Native TCP(9000) 포트 바인딩
3. `clickhouse_data` 볼륨으로 데이터 영속화
4. `schema.sql`을 `/docker-entrypoint-initdb.d/schema.sql`에 마운트해 초기 스키마 적용
5. `clickhouse-schema-sync` one-shot 서비스로 기존 볼륨에도 schema를 재반영 가능
6. 기본 테이블 생성:
   - `default.taxi_events` (원본 이벤트 적재)
   - `default.taxi_zones` (zone 메타/좌표 테이블)
   - `default.taxi_predictions` (Flink ONNX 예측 결과 적재)
7. dedup serving 레이어 생성:
   - `default.taxi_events_serving`, `default.taxi_predictions_serving` (`ReplacingMergeTree`)
   - `default.mv_taxi_events_to_serving`, `default.mv_taxi_predictions_to_serving` (raw -> serving MV)
   - `default.taxi_events_latest`, `default.taxi_predictions_latest` (`FINAL` 조회 뷰)
8. `/ping` 헬스체크로 생존 확인

참고:
- Demolish Ops 이후 `infra/clickhouse/config/default.yaml` 의존은 제거되었고,
  runtime 값은 compose 환경변수(`TZ`)로 관리한다.

## Design decisions
| Decision | Why | Trade-off |
|---|---|---|
| 고정 이미지 버전 23.8 | 재현 가능한 환경 보장 | 최신 패치 지연 가능 |
| 데이터 볼륨 영속화 | 재시작/재배포 시 데이터 보존 | 로컬 디스크 사용 증가 |
| `schema.sql` mount 초기화 | 테이블 bootstrap 자동화 | schema 변경 재적용 절차 별도 필요 |
| `clickhouse-schema-sync` one-shot | 기존 volume에도 schema drift 없이 반영 | 실행 순서(runbook) 관리 필요 |
| raw/serving 레이어 분리 | JDBC at-least-once 중복 가능성과 조회 정합성 분리 | MV/FINAL 조회 비용 증가 |
| `/ping` 헬스체크 | 빠른 상태 확인 | 쿼리 레벨 검증은 별도 필요 |

## Failure modes
| Failure | Detection | Response |
|---|---|---|
| 포트 충돌(8123/9000) | `docker compose up` 에러/로그 | 포트 매핑 변경 |
| 볼륨 권한/손상 | 컨테이너 로그 오류 | 볼륨 권한 확인 또는 재생성 |
| 헬스체크 실패 | `unhealthy` 상태 | 로그 확인 후 재시작/리소스 점검 |
| 초기 스키마 미적용 | 테이블 없음/로그 | `schema.sql` 경로/내용 확인 |
| 기존 volume schema drift | 신규 객체(`*_latest`) 미존재 | `clickhouse-schema-sync` 실행으로 schema 재반영 |
| raw duplicate 증가 | raw duplicate group 쿼리 증가 | 운영 조회는 `taxi_events_latest`/`taxi_predictions_latest` 기준으로 전환 |
| 예측 테이블 적재 없음 | `taxi_events` 증가 대비 `taxi_predictions=0` | Flink prediction sink 설정/ONNX 로드/lag 상태 누적 여부 점검 |
