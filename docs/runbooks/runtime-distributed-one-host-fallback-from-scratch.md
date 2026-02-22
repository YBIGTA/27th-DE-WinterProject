# Runtime Runbook - Distributed One-host Fallback (From Scratch)

이 문서는 distributed compose/설정을 "1대 머신"에서 사전 검증할 때 사용합니다.
목적은 "실 분산 배포 전 설정 검증"이며, 실제 네트워크 분산 검증을 대체하지 않습니다.
관측 경로도 필요 시 동일 머신에서 함께 검증할 수 있습니다.

## 0. 언제 이 문서를 쓰는가

다음 조건이면 이 runbook을 사용합니다.

1. distributed compose를 그대로 써서 빠르게 리허설하고 싶다
2. Kafka 3-broker / Flink distributed 설정이 파싱/기동 가능한지 먼저 확인하고 싶다
3. 아직 멀티 머신 인프라를 띄우기 전이다

## 1. 사전 준비

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject
```

필수 도구:

- Docker + Docker Compose v2
- Java + Maven
- `uv`, `conan`, `cmake`

네트워크 생성 (distributed compose 공통 전제):

```bash
docker network create kafka-network || true
docker network ls | rg kafka-network
```

## 2. 환경파일 준비 (핵심)

권장:

```bash
cp config/.env.one-host-test config/.env
```

대안(수동 구성):

1. `cp config/.env.distributed config/.env`
2. 모든 `*_IP`를 해당 머신에서 접근 가능한 동일 IP로 통일
3. `FLINK_IP=flink-jobmanager`로 오버라이드
4. `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP`는 host-reachable 단일 IP로 유지
5. `FLINK_JOBMANAGER_RPC_PORT`는 기본 `6129` 유지 (커스텀 시 Flink 전 노드 동일값)

검증:

```bash
rg -n "^(KAFKA_[123]_IP|CLICKHOUSE_IP|NGINX_IP|INGESTOR_[123]_IP|FLINK_IP|FLINK_TASKMANAGER_[123]_IP|GENERATOR_IP|LOKI_IP|PROMETHEUS_IP|GRAFANA_IP)=" config/.env
```

기대:

- `FLINK_IP=flink-jobmanager`
- `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP`는 동일 host IP
- 나머지 IP는 하나의 host-reachable IP로 통일

## 3. compose 파싱 검증

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config >/dev/null
```

## 4. 한 머신에서 전체 기동 순서

### 4.1 Kafka 3-broker + topic-init

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1 kafka-2 kafka-3 kafka-ui
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-topic-init
```

검증:

```bash
docker logs --tail 100 kafka-topic-init
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
docker exec kafka-1 kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name taxi-event-data --describe
```

### 4.2 ClickHouse + 스키마 동기화

```bash
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse clickhouse-schema-sync
curl -sf http://localhost:8123/ping
```

### 4.3 Ingestor + Nginx

```bash
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up --build ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up nginx-lb
```

검증:

```bash
curl -sf http://localhost:8081/health
curl -sf http://localhost:8082/health
curl -sf http://localhost:8083/health
curl -sf http://localhost:8080/health
```

### 4.4 Flink

```bash
cd services/flink-job
mvn clean package
cd ../..

docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
```

검증:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
docker logs --tail 100 flink-jobmanager
```

### 4.5 Generator

```bash
cd services/generator
uv --project ../../data run conan profile detect --force
uv --project ../../data run conan install . -of build --build=missing
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build

mkdir -p data
./build/generate config/default.yaml 2>&1 | tee data/generator.log
```

## 5. 최소 성공 검증

```bash
C1=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
sleep 10
C2=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
echo "events_delta_10s=$((C2-C1))"
```

성공 기준:

1. topic shape `12/3/min.insync=2`
2. Flink Job `RUNNING`
3. `events_delta_10s > 0`

## 6. Observability 기동 (선택 권장)

```bash
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env up
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env up
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env up
```

검증:

```bash
curl -sf http://127.0.0.1:3100/ready
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
curl -sf "http://127.0.0.1:${NGINX_PROMTAIL_PORT:-9085}/metrics" >/dev/null
```

## 7. 실 분산 전환 시 반드시 되돌릴 값

실제 멀티 머신으로 전환하기 전에 `config/.env`에서 아래를 복원해야 합니다.

1. `FLINK_IP=flink-jobmanager` -> 실제 JobManager 호스트 IP
2. `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP` -> 각 TaskManager 호스트 IP
3. 모든 `*_IP` -> 각 머신의 실제 IP
