# Distributed Setup Guide (TEMP)

> **주의**: 실제 IP 주소는 config/.env 파일 참조

## 사전 준비 (모든 머신)

```bash
# 1. kafka-network 생성
docker network create kafka-network

# 2. config/.env 파일 배포 (모든 머신에 동일한 파일)
```

## 실행 명령어 (머신별)

### Generator (Build needed)

```bash
cd services/generator
./build/generate
```


### Grafana Loki Prometheus
```bash
# Loki
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d loki

# Prometheus
cd infra/prometheus
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d

# Grafana
cd infra/grafana
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d

# Kafka UI (선택사항)
cd infra/kafka
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d kafka-ui

# Promtail (로컬 로그 수집)
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

### Kafka 머신

```bash
# Kafka 1-2
cd infra/kafka
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d kafka-1 kafka-2

# Kafka-3
cd infra/kafka
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d kafka-3

# Promtail
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

### Nginx LB

```bash
# Kafka 1-2
cd infra/nginx
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d

# Promtail
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

### Ingestor 머신

```bash
# Ingestor 1-2
cd services/ingestor
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d ingestor-1 ingestor-2

# Ingestor-3
cd services/ingestor
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d ingestor-3

# Promtail
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```


### ClickHouse 머신

```bash
# ClickHouse
cd infra/clickhouse
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d

# Promtail
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

### Flink 머신

```bash
# Flink JobManager
cd infra/flink
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d

# Promtail
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

## 헬스체크

```bash
# Generator 머신에서 실행
./health-check.sh
```

## 테스트

```bash
# 데이터 전송 테스트
curl -X POST http://${NGINX_IP}:${NGINX_LB_PORT}/ingest/batch \
  -H "Content-Type: application/json" \
  -d '[{
    "event": "PICKUP",
    "trip_id": 999,
    "ts": "2026-02-10T00:00:00Z",
    "lat": 40.7589,
    "lon": -73.9851,
    "PULocationID": 237
  }]'

# Kafka 토픽 확인
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic taxi-event-data \
  --from-beginning \
  --max-messages 1
```

## 접속 URL

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:${PROMETHEUS_PORT}
- **Kafka UI**: http://localhost:${KAFKA_UI_PORT}

## 중지 (각 머신에서 역순으로)

```bash
# 컴포넌트별 중지
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env down
```

## 트러블슈팅 Quick Commands

```bash
# 컨테이너 상태
docker ps -a

# 로그 확인
docker logs <container-name> --tail 50

# 네트워크 확인
docker network inspect kafka-network

# 헬스체크
curl http://${SERVICE_IP}:${SERVICE_PORT}/health
```
