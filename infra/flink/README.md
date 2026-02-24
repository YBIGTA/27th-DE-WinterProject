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
# distributed는 config/.env.distributed를 기반으로 실제 머신 IP/PORT를 반영
# one-host distributed fallback 시에는 history 문서대로 FLINK_IP=flink-jobmanager 오버라이드 사용
# multi-host distributed에서 observability까지 사용 시 FLINK_TASKMANAGER_1_IP/2_IP/3_IP도 각 TM 호스트 IP로 설정
```

## Job 빌드 & 실행 (Application Mode)

Job JAR을 이미지에 포함시켜 `docker compose up` 시 자동으로 Job이 시작된다.

```bash
# 1. single-machine — 이미지 빌드 + 컨테이너 시작 (Job 자동 실행)
# 참고: Dockerfile 빌드 단계에서 mvn clean package가 자동 실행됨
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up --build

# 2. distributed — 각 Flink 머신에서 최신 코드 기준으로 자기 서비스만 기동
# 참고: 실행 전 각 머신에서 git pull로 커밋을 맞춘다.
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-jobmanager

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-1

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-2

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-taskmanager-3
```

## Distributed 기본 처리량 설정
- distributed는 single-machine과 동일하게 `parallelism=12` 기준으로 고정합니다.
- TaskManager는 3대 x `taskmanager.numberOfTaskSlots=4`로 총 12 슬롯을 제공합니다.
- Kafka 기본 파티션 12와 1:1 병렬 처리를 목표로 맞춘 설정입니다.
- distributed 기본 연결값은 `${KAFKA_1_IP}:${KAFKA_1_EXTERNAL_PORT}`, `${KAFKA_2_IP}:${KAFKA_2_EXTERNAL_PORT}`, `${KAFKA_3_IP}:${KAFKA_3_EXTERNAL_PORT}`, `${CLICKHOUSE_IP}`입니다.

## ONNX/Prediction 관련 주요 환경변수
- `FLINK_ENABLE_CLICKHOUSE_SINK` (기본 `true`)
- `FLINK_ENABLE_PREDICTION_SINK` (기본 `true`)
- `FLINK_CLICKHOUSE_PREDICTION_TABLE` (기본 `taxi_predictions`)
- `FLINK_KAFKA_BOOTSTRAP_SERVERS` (기본 `${KAFKA_n_IP}:${KAFKA_n_EXTERNAL_PORT}` 조합)
- `FLINK_CLICKHOUSE_HOST` (기본 `${CLICKHOUSE_IP}`)
- `FLINK_KAFKA_START_OFFSETS` (기본 `committed`, 지원값: `committed|earliest|latest`)
- `FLINK_WINDOW_DEMAND_MINUTES` (기본 `3`)
- `FLINK_CLICKHOUSE_SINK_PARALLELISM` (기본 `FLINK_PARALLELISM`)
- `FLINK_ONNX_MODEL_PATH` (기본 `/opt/flink/model/taxi_demand_model.onnx`)
- `FLINK_MODEL_VERSION` (기본 `onnx_v1`)
- `FLINK_MODEL_FEATURE_LAG_STEPS` (기본 `20`)
- `FLINK_MODEL_HORIZON_STEPS` (기본 `5`)
- `FLINK_MODEL_INTERVAL_MINUTES` (기본 `3`)

single-machine/distributed compose 모두 Flink 컨테이너에 `../../model/models:/opt/flink/model:ro`를 마운트한다.
single-machine/distributed compose 모두 Flink 컨테이너에 `../../services/flink-job/data:/opt/flink/data`를 마운트한다.

주의:
- 모델 파일 실제 존재 여부는 컨테이너 안에서 반드시 확인한다. (`/opt/flink/model/taxi_demand_model.onnx`)
- DLQ 디렉토리 권한이 낮으면 Flink DLQ가 `/tmp/dead_letter_queue-<hostname>.jsonl` fallback으로 기록될 수 있다.

DLQ 확인:
```bash
# 호스트 DLQ 디렉토리 준비 (권한 이슈 방지)
mkdir -p services/flink-job/data
chmod 777 services/flink-job/data

# 호스트에서 DLQ 파일 확인
ls -la services/flink-job/data
for f in services/flink-job/data/dead_letter_queue-*.jsonl; do
  echo "$f $(wc -l < "$f")"
done
```

전달 보장 참고:
- Source offset 시작점은 `FLINK_KAFKA_START_OFFSETS`로 제어한다.
  - `committed`는 커밋 오프셋이 있으면 그 지점부터, 없으면 earliest부터 시작한다.
- JDBC sink는 at-least-once 특성을 가지므로 raw 테이블(`taxi_events`, `taxi_predictions`)에는 재시도 구간 중복이 남을 수 있다.
- 중복 완화 조회는 ClickHouse serving 레이어(`taxi_events_latest`, `taxi_predictions_latest`) 기준으로 수행한다.

실행 모드 참고:
- Real distributed: 각 머신에서 실제 reachable host IP를 사용한다.
- Real distributed: `FLINK_JOBMANAGER_RPC_PORT`를 모든 Flink 노드에서 동일하게 맞추고, JobManager 호스트에서 해당 포트를 외부로 열어야 한다.
- One-host distributed fallback: `FLINK_IP=flink-jobmanager`를 적용하고, `FLINK_TASKMANAGER_*_IP` 및 나머지 `*_IP` 키는 host-reachable 단일 IP로 맞춘다.

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
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_events_latest"
docker exec -i clickhouse clickhouse-client --query "SELECT count(*) FROM default.taxi_predictions_latest"
docker exec -i clickhouse clickhouse-client --query "SELECT prediction_time,target_time,zone_id,predicted_demand,model_version FROM default.taxi_predictions ORDER BY prediction_time DESC LIMIT 20"
```
