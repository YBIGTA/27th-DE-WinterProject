# Runtime Runbook - Stop/Reset (Distributed)

이 문서는 멀티 머신 분산 환경에서의 정지/초기화 절차입니다.
모든 명령은 "해당 서비스가 실제로 떠 있는 머신"에서 실행하세요.

## 0. 모드 선택

| 모드 | 데이터 보존 | 용도 |
|---|---|---|
| 안전 정지 | 유지 | 일시 중단 |
| 완전 정리 | 삭제 | 분산 환경 재구축 |

## 1. 안전 정지 (권장 순서)

### 1.1 Generator (머신 G)

```bash
pkill -f "generate" || true
```

### 1.2 Observability (머신 H, 사용 중일 때만)

```bash
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env down
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env down
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env down
```

### 1.3 Flink (머신 F)

```bash
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env down
```

### 1.4 Nginx + Ingestor (머신 E)

```bash
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env down
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env down
```

### 1.5 ClickHouse (머신 D)

```bash
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env down
```

### 1.6 Kafka (머신 C -> B -> A 순서 권장)

```bash
# 머신 C
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env down

# 머신 B
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env down

# 머신 A
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env down
```

## 2. 완전 정리 (볼륨 포함 삭제)

주의: 아래 명령은 데이터가 영구 삭제됩니다.

### 2.1 Observability (H)

```bash
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
```

### 2.2 Core pipeline

머신 F:

```bash
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
```

머신 E:

```bash
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
```

머신 D:

```bash
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
```

머신 C/B/A:

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env down -v --remove-orphans
```

Generator (G):

```bash
pkill -f "generate" || true
```

선택: 각 머신에서 docker network까지 제거

```bash
docker network rm kafka-network || true
```

## 3. 부분 초기화 예시

ClickHouse 호스트(D)에서 데이터만 비우기:

```bash
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_events"
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_predictions"
```

Kafka topic shape 재검증(머신 A):

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-topic-init
docker logs --tail 100 kafka-topic-init
```

## 4. 재기동 진입점

- 전체 재기동: `docs/runbooks/runtime-distributed-from-scratch.md`
- one-host fallback 재기동: `docs/runbooks/runtime-distributed-one-host-fallback-from-scratch.md`
