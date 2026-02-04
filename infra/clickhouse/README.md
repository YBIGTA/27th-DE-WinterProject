# ClickHouse (Docker Compose)

로컬/개발 환경에서 ClickHouse 서버를 Docker Compose로 실행하기 위한 가이드입니다.

## Prerequisites
- **Docker + Docker Compose**

## Compose 파일
- `ops/compose/single-machine/clickhouse.yml`
- `ops/compose/distributed/clickhouse.yml`

## Quick start (commands used)
```bash
docker compose -f ops/compose/single-machine/clickhouse.yml up -d
```

### Health check
```bash
curl -s http://localhost:8123/ping
```
Expected: `Ok`

## Ports
- **8123**: HTTP interface
- **9000**: Native TCP interface

## Data persistence
- Docker volume `clickhouse_data` → `/var/lib/clickhouse`

## Optional schema init
- `infra/clickhouse/schema.sql` is mounted to `/docker-entrypoint-initdb.d/schema.sql`
- To re-apply, update the file and restart the container.

## Stop
```bash
docker compose -f ops/compose/single-machine/clickhouse.yml down
```
