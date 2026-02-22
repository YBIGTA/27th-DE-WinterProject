# Runtime Runbook - Single Machine (From Scratch)

이 문서는 로컬 1대에서 파이프라인 전체를 "처음부터" 올리는 절차입니다.
대상 경로:
`generator -> nginx -> ingestor -> kafka -> flink -> clickhouse`
관측 경로:
`promtail/loki + prometheus + grafana`

## 0. 완료 기준

아래 6가지가 모두 충족되면 성공입니다.

1. `http://localhost:8080/health` 응답이 `200`
2. `docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data` 결과가 `12/3/min.insync=2`
3. `docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"` 값이 시간에 따라 증가
4. Flink Job이 `RUNNING` 상태
5. `http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy` 응답 정상
6. `http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health` 응답 정상

## 1. 사전 준비

필수 도구:

- Docker + Docker Compose v2
- Java + Maven (`services/flink-job` 이미지 빌드 사전 검증용)
- `uv`, `conan`, `cmake` (Generator 최초 빌드용)

프로젝트 루트로 이동:

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject
```

## 2. 환경파일 준비 (`config/.env`)

single-machine 템플릿으로 시작:

```bash
cp config/.env.single-machine config/.env
```

검증 1: `.env`가 network-only 규칙을 지키는지 확인

```bash
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'
```

기대 결과: 출력 없음

검증 2: compose 파싱 확인

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.yml --env-file config/.env config >/dev/null
```

기대 결과: 에러 없음

## 3. Kafka 기동 + topic shape 가드레일

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d kafka-topic-init
```

검증:

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env ps
docker logs --tail 100 kafka-topic-init
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
docker exec kafka-1 kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name taxi-event-data --describe
```

기대 결과:

1. `PartitionCount: 12`
2. `ReplicationFactor: 3`
3. `min.insync.replicas=2`
4. `kafka-topic-init` 로그에 `Topic shape verified`

## 4. ClickHouse 기동 + 스키마 동기화

```bash
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up -d
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up -d clickhouse-schema-sync
```

검증:

```bash
curl -sf http://localhost:8123/ping
# expected: Ok

docker logs --tail 100 clickhouse-schema-sync
```

참고:

- 볼륨이 이미 존재하는 경우에도 `clickhouse-schema-sync`를 다시 실행하면 스키마 드리프트를 줄일 수 있습니다.

## 5. Ingestor 3개 기동

```bash
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up -d --build
```

검증:

```bash
curl -sf http://localhost:8081/health
curl -sf http://localhost:8082/health
curl -sf http://localhost:8083/health
```

## 6. Nginx LB 기동

```bash
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up -d
curl -sf http://localhost:8080/health
```

## 7. Flink 기동

선택(권장): 코드 변경 여부와 관계없이 로컬 빌드로 사전 실패 확인

```bash
cd services/flink-job
mvn clean package
cd ../..
```

기동:

```bash
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up -d --build
```

검증:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
docker logs --tail 100 flink-jobmanager
docker logs --tail 100 flink-taskmanager-1
```

## 8. Generator 최초 빌드 및 실행

최초 1회 빌드:

```bash
cd services/generator
uv --project ../../data run conan profile detect --force
uv --project ../../data run conan install . -of build --build=missing
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

실행(로그 파일 포함 권장):

```bash
cd services/generator
mkdir -p data
./build/generate config/default.yaml 2>&1 | tee data/generator.log
```

## 9. E2E 스모크 체크

Generator가 동작 중인 상태에서 별도 터미널에서 실행:

```bash
curl -sf http://localhost:8080/health

C1=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
sleep 10
C2=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events")
echo "events_delta_10s=$((C2-C1))"

docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events_latest"
docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_predictions"
```

성공 기준:

1. `events_delta_10s > 0`
2. `taxi_events_latest` 조회 성공
3. Flink 로그에 지속적인 에러 루프 없음

## 10. Observability 기동 (기본 포함 단계)

```bash
# Loki + Promtail
docker compose -f infra/loki/docker-compose.yml --env-file config/.env up -d

# Prometheus
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env up -d

# Grafana
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env up -d
```

검증:

```bash
curl -sf http://127.0.0.1:3100/ready
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
```

## 11. 바로 이어서 할 작업

1. 관측 스택 상세/단독 복구는 `docs/runbooks/runtime-observability-single-machine.md`
2. 전체 검증 체크리스트는 `docs/runbooks/validation.md`
3. 종료/초기화는 `docs/runbooks/runtime-stop-reset-single-machine.md`
