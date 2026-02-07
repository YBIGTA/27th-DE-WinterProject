# Ingestor Runtime Guide

## 핵심 파일
- App bootstrap: `services/ingestor/src/main/resources/application.yml`
- Compose (single): `services/ingestor/docker-compose.yml`
- Compose (distributed): `services/ingestor/docker-compose.distributed.yml`

Ingestor runtime 값(topic/tuning)은 compose의 `APP_*` 환경변수로 주입한다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## Single-machine 기동
```bash
# Kafka 먼저
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up

# Ingestor 3개
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up

# Nginx LB
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up
```

## Distributed 기동
포그라운드 실행 기준이므로 해당 터미널을 유지한다.
```bash
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up ingestor-1 ingestor-2 ingestor-3
```

## 상태/로그
```bash
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env ps ingestor-1 ingestor-2 ingestor-3
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env logs -f ingestor-1 ingestor-2 ingestor-3
```

## Generator 연동
```bash
cd services/generator
./build/generate
```
