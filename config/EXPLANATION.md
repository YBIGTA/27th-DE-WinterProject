---
component: Environment-Based Deployment Configuration System
status: CURRENT
last_reviewed: 2026-02-04
pipeline: generator -> nginx -> ingestor -> kafka -> (s3 sink connector -> S3) / (flink -> clickhouse)
core_files:
  - config/.env.single-machine
  - config/.env.distributed
  - config/README.md
  - infra/kafka/docker-compose.yml
  - infra/kafka/docker-compose.distributed.yml
  - infra/clickhouse/docker-compose.yml
  - infra/clickhouse/docker-compose.distributed.yml
  - services/ingestor/docker-compose.yml
  - services/ingestor/docker-compose.distributed.yml
  - infra/nginx/docker-compose.yml
  - infra/nginx/docker-compose.distributed.yml
  - infra/flink/docker-compose.yml
  - infra/flink/docker-compose.distributed.yml
---

# Environment-Based Config System

## 목표
하나의 코드베이스로 single-machine / distributed를 모두 지원하면서,
설정 책임을 네트워크와 런타임 튜닝으로 분리한다.

## 규칙
1. `config/.env`는 `*_IP`, `*_PORT`만 유지
2. 배포 서비스의 non-network 설정은 compose `environment`에 하드코딩
3. compose 실행 시 `--env-file config/.env` 명시

## Runtime wiring
| Component | Runtime config load path | 방식 |
|---|---|---|
| Kafka | `infra/kafka/docker-compose.*.yml` | broker tuning values are hardcoded in compose environment |
| ClickHouse | `infra/clickhouse/docker-compose.*.yml` | runtime env in compose (`TZ`) + schema mount |
| Nginx (SM) | `infra/nginx/nginx.single-machine.conf` | static mount |
| Nginx (DM) | `infra/nginx/templates/nginx.distributed.conf.template` | `envsubst` for `.env` IP/PORT only |
| Ingestor | `services/ingestor/docker-compose.*.yml` | Spring properties via env (`APP_*`, `SPRING_*`) |
| Flink | `infra/flink/docker-compose.*.yml` | Job config via env (`FLINK_*`) |
| Generator | `services/generator/config/default.yaml` | native C++ YAML parser (`services/generator/generate.cpp`) |

## Compose 변수 치환 주의
분산 모드 `${KAFKA_1_IP}` 같은 값은 compose 파싱 시점 치환값이다.
서비스 `env_file:`만으로는 치환 시점이 보장되지 않는다.

정상 패턴:
```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1
```

## Invariants
1. `config/.env`에는 `*_IP`, `*_PORT`만 존재
2. distributed에서는 모든 머신이 동일한 `config/.env` 사용
3. Ingestor/Flink topic 일치
4. Flink ClickHouse target(`database.table`)은 `infra/clickhouse/schema.sql`와 일치

## Operational checks
```bash
# network-only 확인
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'

# compose 파싱 검증
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.yml --env-file config/.env config >/dev/null
```

## 주요 실패 패턴
| Failure | Symptom | 대응 |
|---|---|---|
| `--env-file` 누락 | `${VAR}` blank/default 경고 | 실행 커맨드에 `--env-file config/.env` 추가 |
| `.env`에 non-network 키 혼재 | source-of-truth 충돌 | `.env`에서 제거 |
| topic 불일치 | Kafka -> Flink 데이터 미유입 | compose의 `APP_KAFKA_TOPIC`, `FLINK_KAFKA_TOPIC` 통일 |
| ClickHouse table 불일치 | Flink JDBC insert 실패 | `FLINK_CLICKHOUSE_DATABASE/TABLE`과 schema 정합성 확인 |
