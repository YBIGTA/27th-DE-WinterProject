---
component: clickhouse-docker-compose
status: CURRENT
last_reviewed: 2026-02-04
core_files:
  - infra/clickhouse/docker-compose.yml
  - infra/clickhouse/docker-compose.distributed.yml
  - infra/clickhouse/schema.sql
---

# clickhouse-docker-compose

## Role
로컬/개발 환경에서 ClickHouse 서버를 Docker Compose로 구동하고 데이터와 초기 스키마를 관리한다.

## I/O Flow
```
[Client Apps] --(HTTP 8123 / TCP 9000)--> [ClickHouse Container] --(Filesystem)--> [clickhouse_data Volume]
```

## Core behavior
1. ClickHouse 컨테이너(`clickhouse/clickhouse-server:23.8`) 실행
2. HTTP(8123), Native TCP(9000) 포트 바인딩
3. `clickhouse_data` 볼륨으로 데이터 영속화
4. `schema.sql`을 `/docker-entrypoint-initdb.d/schema.sql`에 마운트해 초기 스키마 적용
5. `/ping` 헬스체크로 생존 확인

참고:
- Demolish Ops 이후 `infra/clickhouse/config/default.yaml` 의존은 제거되었고,
  runtime 값은 compose 환경변수(`TZ`)로 관리한다.

## Design decisions
| Decision | Why | Trade-off |
|---|---|---|
| 고정 이미지 버전 23.8 | 재현 가능한 환경 보장 | 최신 패치 지연 가능 |
| 데이터 볼륨 영속화 | 재시작/재배포 시 데이터 보존 | 로컬 디스크 사용 증가 |
| `schema.sql` mount 초기화 | 테이블 bootstrap 자동화 | schema 변경 재적용 절차 별도 필요 |
| `/ping` 헬스체크 | 빠른 상태 확인 | 쿼리 레벨 검증은 별도 필요 |

## Failure modes
| Failure | Detection | Response |
|---|---|---|
| 포트 충돌(8123/9000) | `docker compose up` 에러/로그 | 포트 매핑 변경 |
| 볼륨 권한/손상 | 컨테이너 로그 오류 | 볼륨 권한 확인 또는 재생성 |
| 헬스체크 실패 | `unhealthy` 상태 | 로그 확인 후 재시작/리소스 점검 |
| 초기 스키마 미적용 | 테이블 없음/로그 | `schema.sql` 경로/내용 확인 |
