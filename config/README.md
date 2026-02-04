# Config Runbook (Phase 2)

이 문서는 현재 저장소의 **실행 기준 문서**입니다.

## 핵심 규칙
1. `config/.env`는 네트워크(IP/PORT)만 가진다.
2. non-network 값(topic, table, tuning)은 컴포넌트 YAML에서 관리한다.
3. Compose 실행은 항상 `--env-file config/.env`를 같이 쓴다.

## Source of Truth
| Layer | File(s) | Contains |
|---|---|---|
| Network | `config/.env.single-machine`, `config/.env.distributed`, `config/.env` | IP/PORT only |
| Ingestor app | `services/ingestor/config/default.yaml` | topic + tuning |
| Generator app | `services/generator/config/default.yaml` | ingestion fallback + tuning |
| Flink app | `services/flink-job/config/default.yaml` | topic + ClickHouse target + tuning |
| Kafka infra | `infra/kafka/config/default.yaml` | broker runtime tuning |
| ClickHouse infra | `infra/clickhouse/config/default.yaml` | runtime tuning |
| Nginx infra | `infra/nginx/config/default.yaml` | upstream/proxy tuning |

## 왜 `--env-file`이 필요한가
분산 compose는 `${KAFKA_1_IP}` 같은 변수를 compose 파싱 시점에 치환합니다.
서비스의 `env_file:`만으로는 이 치환이 보장되지 않으므로, 실행 커맨드에 `--env-file config/.env`를 명시해야 합니다.

## Standard Command Pattern
모든 실행은 프로젝트 루트에서 아래 패턴 사용:

```bash
docker compose -f ops/compose/<mode>/docker-compose.yml --env-file config/.env up -d <service...>
```

- `<mode>`: `single-machine` 또는 `distributed`

## 1) Single-machine

```bash
# 0. 템플릿 활성화
cp config/.env.single-machine config/.env

# 1. Kafka

docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3

# 2. ClickHouse
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d clickhouse

# 3. Ingestor + Nginx
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3 nginx-lb

# 4. Flink
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink

# 5. Kafka UI (optional)
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d kafka-ui

# 6. Generator (native)
cd services/generator && ./build/generate
```

## 2) Distributed

```bash
# 0. 템플릿 활성화
cp config/.env.distributed config/.env
# 이후 config/.env에 실제 IP/PORT 입력

# 1. Kafka brokers

docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3

# 2. ClickHouse
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d clickhouse

# 3. Ingestor + Nginx
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3 nginx-lb

# 4. Flink
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink

# 5. Kafka UI (optional)
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d kafka-ui

# 6. Generator (native)
cd services/generator && ./build/generate
```

## 보장 범위
`docker compose up <service>`만으로 자동 보장되지 않는 항목:
1. `.env` compose 치환값 로딩 (`--env-file` 필요)
2. 선행 의존 서비스 준비 (예: Kafka 없이 ingestor 단독)
3. Generator 실행 (compose 서비스 아님)

## 빠른 검증
```bash
# .env network-only 확인
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'

# compose 치환 검증

docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env config >/dev/null

docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env config >/dev/null
```

## Do not commit
실사용 값이 들어간 `config/.env`는 커밋하지 않는다.
