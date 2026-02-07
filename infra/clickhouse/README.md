# ClickHouse Runtime Guide

## 파일 위치
- Single-machine compose: `infra/clickhouse/docker-compose.yml`
- Distributed compose: `infra/clickhouse/docker-compose.distributed.yml`
- Schema: `infra/clickhouse/schema.sql`

ClickHouse runtime 값은 compose에 하드코딩되어 있고, schema는 mount로 초기화된다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## 실행
모든 compose 실행은 포그라운드 기준이다.
```bash
# single-machine
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up

# distributed
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse
```

## 헬스체크
```bash
curl -s http://localhost:8123/ping
# expected: Ok
```

## 포트
- HTTP: `8123`
- Native TCP: `9000`

## 중지
```bash
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env stop clickhouse
```
