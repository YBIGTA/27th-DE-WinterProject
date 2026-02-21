# Flink Runtime Guide

## 파일 위치
- Single-machine compose: `infra/flink/docker-compose.yml`
- Distributed compose: `infra/flink/docker-compose.distributed.yml`
- Job source: `services/flink-job`

Flink job runtime 값(parallelism/topic/jdbc/table 등)은 compose의 `FLINK_*` 환경변수에서 로딩한다.
현재 Job은 원본 이벤트 적재(`taxi_events`)와 ONNX 기반 예측 적재(`taxi_predictions`)를 동시에 수행한다.
`taxi_events` 적재 라인은 집계/예측 라인과 분리되어 동작하며, 원본 이벤트 스트림을 직접 JDBC sink로 저장한다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## Job 빌드 & 실행 (Application Mode)

Job JAR을 이미지에 포함시켜 `docker compose up` 시 자동으로 Job이 시작된다.

```bash
# 1. JAR 빌드 (최초 또는 코드 변경 시)
cd services/flink-job && mvn clean package && cd ../..

# 2. single-machine — 이미지 빌드 + 컨테이너 시작 (Job 자동 실행)
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up --build

# distributed
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-jobmanager

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-1

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-2

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-3
```

## ONNX/Prediction 관련 주요 환경변수
- `FLINK_ENABLE_PREDICTION_SINK` (기본 `true`)
- `FLINK_CLICKHOUSE_PREDICTION_TABLE` (기본 `taxi_predictions`)
- `FLINK_ONNX_MODEL_PATH` (기본 `/opt/flink/model/taxi_demand_model.onnx`)
- `FLINK_MODEL_VERSION` (기본 `onnx_v1`)
- `FLINK_MODEL_FEATURE_LAG_STEPS` (기본 `20`)
- `FLINK_MODEL_HORIZON_STEPS` (기본 `5`)
- `FLINK_MODEL_INTERVAL_MINUTES` (기본 `3`)

single-machine compose에서는 모든 Flink 컨테이너에 `../../model/models:/opt/flink/model:ro`가 마운트된다.

주의:
- 현재 `infra/flink/docker-compose.distributed.yml` 기준 `flink-taskmanager-3`에는 모델 볼륨 마운트가 없어, 예측 오퍼레이터가 해당 노드에 배치되면 ONNX 초기화 실패가 발생할 수 있다.

## 확인
```bash
# single-machine
docker logs -f flink-taskmanager-1

# distributed
docker logs -f flink-taskmanager-1

docker logs -f flink-taskmanager-2

docker logs -f flink-taskmanager-3
```

또는

http://localhost:8084

## 적재 확인 (ClickHouse)
```bash
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_events"
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_predictions"
docker exec -i clickhouse clickhouse-client --query "SELECT prediction_time,target_time,zone_id,predicted_demand,model_version FROM default.taxi_predictions ORDER BY prediction_time DESC LIMIT 20"
```
