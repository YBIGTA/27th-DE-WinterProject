# ClickHouse Runtime Guide

## 파일 위치
- Single-machine compose: `ops/compose/single-machine/docker-compose.yml`
- Distributed compose: `ops/compose/distributed/docker-compose.yml`
- Runtime tuning YAML: `infra/clickhouse/config/default.yaml`
- Schema: `infra/clickhouse/schema.sql`

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## 실행
```bash
# single-machine
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d clickhouse

# distributed
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d clickhouse
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
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env stop clickhouse
```
