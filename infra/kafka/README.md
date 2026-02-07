# Kafka Runtime Guide

## 파일 위치
- Single-machine compose: `infra/kafka/docker-compose.yml`
- Distributed compose: `infra/kafka/docker-compose.distributed.yml`

Kafka broker tuning 값은 compose `environment`에 하드코딩되어 있다.

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
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up
```

## Distributed 실행 (머신별)
각 명령은 포그라운드 실행이므로 머신별 터미널에서 유지한다.
```bash
# machine-1
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1

# machine-2
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-2

# machine-3
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-3 kafka-ui
```

## 상태 확인
```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env ps kafka-1 kafka-2 kafka-3 kafka-ui
```

## 토픽 확인
```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
```

## 중지
```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down
```
