# Runtime Runbook - Stop/Reset (Single Machine)

이 문서는 단일 머신 환경의 종료/초기화 절차입니다.

## 0. 모드 선택

| 모드 | 데이터 보존 | 용도 |
|---|---|---|
| 안전 정지 | 유지 | 잠시 중단 후 재기동 |
| 완전 정리 | 삭제 | 처음부터 다시 실행 |
| 선택 초기화 | 부분 삭제 | 테이블만 비우기 등 |

## 1. 안전 정지 (데이터 유지)

Generator 정지:

```bash
pkill -f "generate" || true
```

관측 스택 정지(올렸던 경우):

```bash
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env down
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env down
docker compose -f infra/loki/docker-compose.yml --env-file config/.env down
```

파이프라인 정지:

```bash
docker compose -f infra/flink/docker-compose.yml --env-file config/.env down
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env down
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env down
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env down
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down
```

## 2. 완전 정리 (데이터/볼륨 삭제)

주의: 아래 명령은 Kafka/ClickHouse/Grafana/Loki 데이터까지 삭제합니다.

```bash
pkill -f "generate" || true

docker compose -f infra/grafana/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/loki/docker-compose.yml --env-file config/.env down -v --remove-orphans

docker compose -f infra/flink/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down -v --remove-orphans
```

선택: 네트워크까지 지우고 완전 초기상태로 만들기

```bash
docker network rm kafka-network || true
```

## 3. 선택 초기화 (컨테이너 유지)

### 3.1 ClickHouse 테이블만 비우기

```bash
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_events"
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_predictions"
```

### 3.2 Kafka topic shape 재검증만 수행

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d kafka-topic-init
docker logs --tail 100 kafka-topic-init
```

## 4. 재기동 진입점

완전 정리 후 재기동은 아래 문서를 사용하세요.

- `docs/runbooks/runtime-single-machine-from-scratch.md`
