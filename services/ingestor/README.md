# Ingestor Runtime Guide

## 핵심 파일
- App config: `services/ingestor/config/default.yaml`
- Spring config import: `services/ingestor/src/main/resources/application.yml`
- Compose entrypoint (single): `ops/compose/single-machine/docker-compose.yml`
- Compose entrypoint (distributed): `ops/compose/distributed/docker-compose.yml`

Ingestor는 `spring.config.import`로 `/app/config/default.yaml`을 읽습니다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## Single-machine 기동
```bash
# Kafka 먼저
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3

# Ingestor 3개 + nginx LB
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3 nginx-lb
```

## Distributed 기동
```bash
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3 nginx-lb
```

## 상태/로그
```bash
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env ps ingestor-1 ingestor-2 ingestor-3 nginx-lb
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env logs -f ingestor-1 ingestor-2 ingestor-3
```

## Generator 연동
```bash
cd services/generator
./build/generate
```

## 로컬 개발(비도커)
```bash
cd services/ingestor
./gradlew clean build
./gradlew bootRun
```
