---
component: Environment-Based Deployment Configuration System
status: CURRENT
last_reviewed: 2026-02-04
pipeline: generator -> nginx -> ingestor -> kafka -> (s3 sink connector -> S3) / (flink -> clickhouse)
core_files:
  - config/.env.single-machine
  - config/.env.distributed
  - config/README.md
  - services/generator/config/default.yaml
  - services/ingestor/config/default.yaml
  - services/flink-job/config/default.yaml
  - infra/kafka/config/default.yaml
  - infra/clickhouse/config/default.yaml
  - infra/nginx/nginx.single-machine.conf
  - infra/nginx/templates/nginx.distributed.conf.template
---

# Environment-Based Config System

## 목표
하나의 코드베이스로 single-machine / distributed를 모두 지원하면서,
설정 책임을 네트워크와 애플리케이션 튜닝으로 분리한다.

## 규칙
1. `config/.env`는 `*_IP`, `*_PORT`만 유지
2. non-network 설정은 컴포넌트 YAML로 이동
3. Compose 실행 시 `--env-file config/.env`를 명시

## Runtime wiring
| Component | Runtime config load path | 방식 |
|---|---|---|
| Kafka | `infra/kafka/config/default.yaml` | `infra/kafka/entrypoint.sh`: YAML -> env export -> confluent run |
| ClickHouse | `infra/clickhouse/config/default.yaml` | native YAML config (`/etc/clickhouse-server/config.d/`) |
| Nginx (SM) | `infra/nginx/nginx.single-machine.conf` | static mount, no env |
| Nginx (DM) | `infra/nginx/templates/nginx.distributed.conf.template` | `envsubst` on `.env` IP/PORT vars only |
| Ingestor | `services/ingestor/config/default.yaml` | Spring `spring.config.import` |
| Flink | `services/flink-job/config/default.yaml` | `FLINK_CONFIG_PATH` + SnakeYAML |
| Generator | `services/generator/config/default.yaml` | C++ YAML parser (`generate.cpp`) |

## Compose 변수 치환 주의
분산 모드의 `${KAFKA_1_IP}` 등은 compose 파싱 시점 치환값이다.
서비스 `env_file:`은 컨테이너 env 주입용이므로 compose 치환을 대신하지 못한다.

정상 패턴:
```bash
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d <service>
```

## Invariants
1. `config/.env`에는 `*_IP`, `*_PORT`만 존재
2. distributed에서는 모든 머신이 동일한 `config/.env` 사용
3. Ingestor topic과 Flink topic은 동일
4. Flink ClickHouse target(`database.table`)은 `infra/clickhouse/schema.sql`와 일치

## Operational checks
```bash
# network-only 확인
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'

# compose 파싱 검증

docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env config >/dev/null

docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env config >/dev/null
```

## 주요 실패 패턴
| Failure | Symptom | 대응 |
|---|---|---|
| `--env-file` 누락 | `${VAR}` blank/default 경고 | 실행 커맨드에 `--env-file config/.env` 추가 |
| `.env`에 non-network 키 혼재 | source-of-truth 충돌 | 해당 YAML로 이동 |
| topic 불일치 | Kafka -> Flink 데이터 미유입 | ingestor/flink YAML topic 통일 |
| ClickHouse 테이블 불일치 | Flink JDBC insert 실패 | Flink YAML + schema.sql 맞춤 |
