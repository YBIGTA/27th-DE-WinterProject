# Runtime Runbook - Distributed Multi-machine (From Scratch)

이 문서는 실제 멀티 머신 분산 배포를 처음부터 수행하는 절차입니다.
모든 머신에서 동일한 `config/.env`를 사용해야 합니다.
관측 경로도 기본 포함합니다:
`promtail/loki + prometheus + grafana`

## 0. 완료 기준

아래 조건이 모두 충족되면 성공입니다.

1. Kafka topic shape가 `12/3/min.insync=2`
2. Nginx `/health` 200, Ingestor 3개 `/health` 200
3. Flink Job `RUNNING`
4. ClickHouse `default.taxi_events` row count가 시간에 따라 증가
5. Prometheus healthy endpoint 응답 정상
6. Grafana health endpoint 응답 정상

## 1. 권장 머신 역할

| 머신 | 역할 | 실행 compose |
|---|---|---|
| A | Kafka-1 + topic-init | `infra/kafka/docker-compose.distributed.yml` |
| B | Kafka-2 | `infra/kafka/docker-compose.distributed.yml` |
| C | Kafka-3 + Kafka UI | `infra/kafka/docker-compose.distributed.yml` |
| D | ClickHouse | `infra/clickhouse/docker-compose.distributed.yml` |
| E | Ingestor-1/2/3 + Nginx LB | `services/ingestor/docker-compose.distributed.yml`, `infra/nginx/docker-compose.distributed.yml` |
| F1 | Flink JobManager | `infra/flink/docker-compose.distributed.yml` |
| F2 | Flink TaskManager-1 | `infra/flink/docker-compose.distributed.yml` |
| F3 | Flink TaskManager-2 | `infra/flink/docker-compose.distributed.yml` |
| F4 | Flink TaskManager-3 | `infra/flink/docker-compose.distributed.yml` |
| G | Generator(native) | `services/generator` |
| H | Loki + Prometheus + Grafana | 기본 포함 단계 (상세는 observability runbook 참고) |

참고:
- 위 표는 "권장 예시 토폴로지"입니다. 고정 규칙이 아닙니다.
- Ingestor는 `ingestor-1/2/3`을 각각 다른 머신(E1/E2/E3)으로 분리 배치할 수 있습니다.
- Nginx LB도 별도 머신(N)으로 분리 가능합니다.

## 2. 모든 머신 공통 준비 (필수)

### 2.1 코드 동기화

모든 머신이 동일 commit/branch여야 합니다.

### 2.2 환경파일 통일

기준 머신에서 `config/.env.distributed`를 복사해 `config/.env`를 만들고, 실제 IP/PORT로 수정한 뒤 동일 파일을 모든 머신에 배포합니다.

```bash
cp config/.env.distributed config/.env
```

중요:

1. `KAFKA_1_IP`, `KAFKA_2_IP`, `KAFKA_3_IP`는 각 Kafka 머신의 실제 접근 IP
2. `CLICKHOUSE_IP`, `NGINX_IP`, `FLINK_IP`, `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP`, `GENERATOR_IP`도 실제 접근 IP
3. `127.0.0.1` 값이 남아 있으면 원격 연결 실패 가능
4. Flink가 단일 머신이면 `FLINK_TASKMANAGER_*_IP`를 `FLINK_IP`와 동일값으로 둬도 됨

쉘에서 `${VAR}` 형태 명령을 쓸 수 있도록 현재 터미널에 환경 로드:

```bash
set -a
source config/.env
set +a
```

### 2.3 Docker 네트워크 생성 (각 머신)

distributed compose는 `kafka-network`를 `external: true`로 가정합니다.
각 머신에서 1회 생성:

```bash
docker network create kafka-network || true
```

검증:

```bash
docker network ls | rg kafka-network
```

### 2.4 compose 파싱 검증 (각 역할 머신에서 실행)

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config >/dev/null
```

## 3. 머신별 기동 순서

### 3.1 Kafka 클러스터 (A/B/C)

머신 A:

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-1
```

머신 B:

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-2
```

머신 C:

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-3 kafka-ui
```

토픽 shape 가드레일 (머신 A):

```bash
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up kafka-topic-init
docker logs --tail 100 kafka-topic-init
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
docker exec kafka-1 kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name taxi-event-data --describe
```

기대 결과: `12/3/min.insync=2`

### 3.2 ClickHouse (D)

```bash
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up clickhouse clickhouse-schema-sync
curl -sf "http://${CLICKHOUSE_IP}:${CLICKHOUSE_HTTP_PORT}/ping"
```

### 3.3 Ingestor + Nginx (E 또는 E1/E2/E3+N)

```bash
# 머신 E1
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up --build ingestor-1

# 머신 E2
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up --build ingestor-2

# 머신 E3
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up --build ingestor-3

# 머신 N
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up nginx-lb
```

검증:

```bash
curl -sf "http://${INGESTOR_1_IP}:${INGESTOR_1_PORT}/health"
curl -sf "http://${INGESTOR_2_IP}:${INGESTOR_2_PORT}/health"
curl -sf "http://${INGESTOR_3_IP}:${INGESTOR_3_PORT}/health"
curl -sf "http://${NGINX_IP}:${NGINX_LB_PORT}/health"
```

### 3.4 Flink (F1/F2/F3/F4)

사전 빌드(각 Flink 머신에서 권장):

```bash
cd services/flink-job
mvn clean package
cd ../..
```

기동:

```bash
# 머신 F1 (JobManager)
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-jobmanager

# 머신 F2 (TaskManager-1)
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-taskmanager-1

# 머신 F3 (TaskManager-2)
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-taskmanager-2

# 머신 F4 (TaskManager-3)
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build flink-taskmanager-3
```

검증:

```bash
# 머신 F1
docker exec flink-jobmanager /opt/flink/bin/flink list
docker logs --tail 100 flink-jobmanager
docker exec flink-jobmanager sh -lc 'wget -qO- http://localhost:8081/jobs/overview'
docker exec flink-jobmanager sh -lc 'wget -qO- http://localhost:8081/taskmanagers'
docker inspect -f '{{.Name}} {{.Image}}' flink-jobmanager

# 머신 F2/F3/F4
docker logs --tail 100 flink-taskmanager-1
docker logs --tail 100 flink-taskmanager-2
docker logs --tail 100 flink-taskmanager-3
docker inspect -f '{{.Name}} {{.Image}}' flink-taskmanager-1
docker inspect -f '{{.Name}} {{.Image}}' flink-taskmanager-2
docker inspect -f '{{.Name}} {{.Image}}' flink-taskmanager-3
```

주의:

- distributed 기본 offset 정책은 `FLINK_KAFKA_START_OFFSETS=committed`
- `FLINK_IP`는 JobManager가 떠 있는 실제 호스트 IP
- `FLINK_JOBMANAGER_RPC_PORT`는 TaskManager가 접속할 JobManager RPC 포트 (기본 `6129`)
- `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP`는 각 TaskManager 호스트 IP이며 Flink가 외부에 광고하는 주소로도 사용됩니다.
- `FLINK_TASKMANAGER_1_DATA_PORT`, `FLINK_TASKMANAGER_2_DATA_PORT`, `FLINK_TASKMANAGER_3_DATA_PORT`는 TaskManager 간 shuffle 데이터 포트입니다.
- 4대(F1~F4)의 Flink 이미지 SHA(`docker inspect -f '{{.Image}}' ...`)는 반드시 동일해야 합니다. 다르면 `Invalid lambda deserialization`로 반복 재시작됩니다.
- `taskmanagers` API 결과에 `172.x.x.x` 같은 컨테이너 내부 IP가 보이면 distributed 네트워크 설정이 잘못된 상태입니다.

### 3.5 Generator (G)

최초 1회 빌드:

```bash
cd services/generator
uv --project ../../data run conan profile detect --force
uv --project ../../data run conan install . -of build --build=missing
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

실행:

```bash
cd services/generator
mkdir -p data
./build/generate config/default.yaml 2>&1 | tee data/generator.log
```

## 4. 공통 E2E 검증

아래 명령은 ClickHouse 호스트(D)에서 실행:

```bash
C1=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
sleep 10
C2=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
echo "events_delta_10s=$((C2-C1))"

docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events_latest"
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_predictions"
```

성공 기준: `events_delta_10s > 0`

## 5. Observability 기동 (H)

```bash
# Loki + Promtail
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env up

# Prometheus
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env up

# Grafana
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env up
```

주의:

- 서비스 로그 수집은 각 distributed compose의 `promtail` sidecar가 담당합니다.
- Nginx 로그 기반 메트릭은 `promtail-nginx`(`http://${NGINX_IP}:${NGINX_PROMTAIL_PORT:-9085}/metrics`)에서 수집됩니다.
- `infra/loki`의 `promtail-loki`는 Loki 호스트 로컬 수집/메트릭 노출 용도입니다.

검증:

```bash
curl -sf "http://${LOKI_IP}:${LOKI_PORT}/ready"
curl -sf "http://${PROMETHEUS_IP}:${PROMETHEUS_PORT}/-/healthy"
curl -sf "http://${GRAFANA_IP}:${GRAFANA_PORT}/api/health"
```

## 6. 분산 포트 체크리스트

방화벽/보안그룹에 최소 아래 포트를 허용하세요.

- Kafka: `9092`, `9094`, `9096`, `29092`, `29094`, `29096`, `19093`, `19094`, `19095`
- Ingestor: `8081`, `8082`, `8083`
- Nginx: `8080`, `9113`, `9085`
- ClickHouse: `8123`, `9000`
- Flink: `8084`, `6129`, `6122`, `6123`, `6124`, `7001`, `7002`, `7003`, `9249`, `9250`, `9251`, `9252`
- Kafka UI: `8090`
- Loki/Promtail: `3100`, `9084`
- Prometheus: `9090`
- Grafana: `3000`

## 7. 바로 이어서 할 작업

1. 관측 스택 상세/단독 복구는 `docs/runbooks/runtime-observability-distributed.md`
2. 전체 검증은 `docs/runbooks/validation.md`
3. 정지/초기화는 `docs/runbooks/runtime-stop-reset-distributed.md`
