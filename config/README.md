# Config Runbook (Demolish Ops)

이 문서는 현재 저장소의 실행 기준 문서입니다.

## 핵심 규칙
1. `config/.env`는 네트워크(IP/PORT)만 가진다.
2. 배포 서비스의 non-network 값(topic, table, tuning)은 각 컴포넌트 compose에 하드코딩한다.
3. 컴포넌트 compose 파일을 직접 실행한다 (root launcher 없음).

## Source of Truth
| Layer | File(s) | Contains |
|---|---|---|
| Network | `config/.env.single-machine`, `config/.env.distributed`, `config/.env` | IP/PORT only |
| Kafka runtime | `infra/kafka/docker-compose.*.yml` | service wiring + broker tuning |
| ClickHouse runtime | `infra/clickhouse/docker-compose.*.yml` | service wiring + runtime env + schema |
| Ingestor runtime | `services/ingestor/docker-compose.*.yml` | service wiring + `APP_*`/`SPRING_*` |
| Nginx runtime | `infra/nginx/docker-compose.*.yml` | service wiring + LB config |
| Flink runtime | `infra/flink/docker-compose.*.yml` | service wiring + `FLINK_*` |
| Generator app config | `services/generator/config/default.yaml` | native generator runtime defaults |

## 왜 `--env-file`이 필요한가
분산 모드는 `${KAFKA_1_IP}` 같은 변수를 compose 파싱 시점에 치환한다.
서비스 `env_file:`은 컨테이너 내부 환경변수 주입용이므로 치환 시점을 대체하지 못한다.

## Standard Command Pattern
프로젝트 루트에서 아래 패턴 사용:

```bash
docker compose -f <component-compose-file> --env-file config/.env up -d <service...>
```

## 1) Single-machine

```bash
# 0. 템플릿 활성화
cp config/.env.single-machine config/.env

# 1. Kafka
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3 kafka-ui

# 2. ClickHouse
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up -d clickhouse

# 3. Ingestor + Nginx
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up -d nginx-lb

# 4. Flink
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink

# 5. Generator (native)
cd services/generator && ./build/generate
```

## 2) Distributed

```bash
# 0. 템플릿 활성화
cp config/.env.distributed config/.env
# 이후 config/.env에 실제 IP/PORT 입력

# Kafka 머신들
# machine-1
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-1
# machine-2
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-2
# machine-3
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-3 kafka-ui

# ClickHouse 머신
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up -d clickhouse

# Ingestor/Nginx 머신
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up -d nginx-lb

# Flink 머신
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d flink-jobmanager flink

# Generator (native)
cd services/generator && ./build/generate
```

## 빠른 검증
```bash
# .env network-only 확인
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'

# compose 파싱 검증
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.yml --env-file config/.env config >/dev/null
```

## Do not commit
실사용 값이 들어간 `config/.env`는 커밋하지 않는다.
