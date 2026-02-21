# ClickHouse Runtime Guide

## 파일 위치
- Single-machine compose: `infra/clickhouse/docker-compose.yml`
- Distributed compose: `infra/clickhouse/docker-compose.distributed.yml`
- Schema: `infra/clickhouse/schema.sql`

ClickHouse runtime 값은 compose에 하드코딩되어 있고, schema는 mount로 초기화된다.
현재 기본 스키마에는 이벤트 원본 테이블(`taxi_events`)과 예측 결과 테이블(`taxi_predictions`)이 포함되어 있다.
중복 완화용 serving 레이어(`taxi_events_serving`, `taxi_predictions_serving`)와 조회 뷰(`taxi_events_latest`, `taxi_predictions_latest`)도 포함된다.

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

기존 volume에도 schema를 반영하려면:
```bash
# single-machine
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up clickhouse-schema-sync

# distributed
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse-schema-sync
```

## 헬스체크
```bash
curl -s http://localhost:8123/ping
# expected: Ok
```

## 테이블 확인
```bash
docker exec -i clickhouse clickhouse-client --query "SHOW TABLES FROM default"
docker exec -i clickhouse clickhouse-client --query "DESCRIBE TABLE default.taxi_events"
docker exec -i clickhouse clickhouse-client --query "DESCRIBE TABLE default.taxi_predictions"
docker exec -i clickhouse clickhouse-client --query "SHOW TABLES FROM default LIKE '%latest'"
```

## 적재 확인 예시
```bash
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_events"
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_predictions"
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_events_latest"
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_predictions_latest"
docker exec -i clickhouse clickhouse-client --query "SELECT prediction_time,target_time,zone_id,predicted_demand,model_version FROM default.taxi_predictions ORDER BY prediction_time DESC LIMIT 20"
```

## 포트
- HTTP: `8123`
- Native TCP: `9000`

## 중지
```bash
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env stop clickhouse
```
