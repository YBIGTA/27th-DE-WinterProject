# Kafka Runtime Guide

## 파일 위치
- Single-machine compose: `ops/compose/single-machine/docker-compose.yml`
- Distributed compose: `ops/compose/distributed/docker-compose.yml`
- Kafka tuning YAML: `infra/kafka/config/default.yaml`

참고: 기존 `infra/kafka/config/tuning.env`는 사용하지 않으며,
현재는 `infra/kafka/config/default.yaml`이 단일 소스입니다.

## 실행 전 준비
```bash
# single-machine
cp config/.env.single-machine config/.env

# distributed
# cp config/.env.distributed config/.env
# 그리고 config/.env에 실제 IP/PORT 반영
```

## Single-machine 실행
```bash
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3 kafka-ui
```

## Distributed 실행
```bash
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3 kafka-ui
```

## 상태 확인
```bash
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env ps kafka-1 kafka-2 kafka-3 kafka-ui
```

## 토픽 확인
```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
```

## 중지
```bash
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env down
# 또는 distributed 파일 경로로 동일하게 실행
```
