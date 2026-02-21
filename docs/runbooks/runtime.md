# Runtime Runbook (Single / Multi Machine)

이 문서는 현재 저장소의 실운영 실행 절차를 싱글 머신/멀티 머신 기준으로 통합 정리한 문서입니다.

## 1. 범위

- Kafka: `infra/kafka/docker-compose*.yml`
- ClickHouse: `infra/clickhouse/docker-compose*.yml`
- Ingestor: `services/ingestor/docker-compose*.yml`
- Nginx LB: `infra/nginx/docker-compose*.yml`
- Flink: `infra/flink/docker-compose*.yml`
- Generator (native): `services/generator`

## 2. 공통 원칙

1. `config/.env`는 IP/PORT만 관리합니다.
2. 컴포넌트 런타임 튜닝 값(topic, batch, parallelism 등)은 각 compose 파일 내부를 소스 오브 트루스로 봅니다.
3. compose는 반드시 `--env-file config/.env`와 함께 실행합니다.
4. 분산 모드는 `${KAFKA_1_IP}` 같은 변수 치환이 필요하므로 `--env-file` 누락 시 실패합니다.

표준 명령 패턴:

```bash
docker compose -f <compose-file> --env-file config/.env up
```

소스 오브 트루스 매트릭스:

| Layer | File(s) | Contains |
|---|---|---|
| Network | `config/.env` | IP/PORT only |
| Kafka runtime | `infra/kafka/docker-compose*.yml` | Broker topology and Kafka tuning |
| ClickHouse runtime | `infra/clickhouse/docker-compose*.yml` | Service wiring and schema bootstrap |
| Ingestor runtime | `services/ingestor/docker-compose*.yml` | `SPRING_*`, `APP_*` |
| Nginx runtime | `infra/nginx/docker-compose*.yml` | Load balancing config |
| Flink runtime | `infra/flink/docker-compose*.yml` | `FLINK_*` runtime values |
| Prometheus runtime | `infra/prometheus/docker-compose*.yml` | Scrape targets, retention |
| Grafana runtime | `infra/grafana/docker-compose*.yml` | Datasources, dashboards |
| Generator runtime | `services/generator/config/default.yaml` | Native generator defaults |

## 3. 사전 준비

## 3.1 필수 도구

- Docker + Docker Compose v2
- Java + Maven (Flink Job JAR 빌드용)
- `uv`, `conan`, `cmake` (Generator 빌드용)

## 3.2 환경파일 선택

```bash
# 단일/분산 공통: 로컬에서 직접 생성/수정
touch config/.env
```

`config/.env` 파일만 git 추적 대상이 아닙니다. (`.gitignore` 정책)

필수 원칙:

- `config/.env`에는 네트워크 값(IP/PORT)만 둡니다.
- topic, batch, parallelism 등 런타임 튜닝은 각 compose 파일 값을 사용합니다.

## 3.3 빠른 검증

```bash
# 1) .env가 network-only 규칙을 지키는지 확인
awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' config/.env | rg -v '(_IP|_PORT)$'

# 2) compose 파싱 확인
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env config >/dev/null
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env config >/dev/null
docker compose -f infra/flink/docker-compose.yml --env-file config/.env config >/dev/null
```

위 명령에서 출력이 비어 있거나 에러가 없으면 정상입니다.

## 3.4 `.env` 최소 필수 키 예시

아래는 single-machine 기준 최소 키 예시입니다.

```dotenv
KAFKA_1_IP=127.0.0.1
KAFKA_2_IP=127.0.0.1
KAFKA_3_IP=127.0.0.1
KAFKA_1_EXTERNAL_PORT=9092
KAFKA_2_EXTERNAL_PORT=9094
KAFKA_3_EXTERNAL_PORT=9096
KAFKA_INTERNAL_PORT=29092

CLICKHOUSE_IP=127.0.0.1
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000

INGESTOR_1_IP=127.0.0.1
INGESTOR_2_IP=127.0.0.1
INGESTOR_3_IP=127.0.0.1
INGESTOR_1_PORT=8081
INGESTOR_2_PORT=8082
INGESTOR_3_PORT=8083

NGINX_IP=127.0.0.1
NGINX_LB_PORT=8080

FLINK_IP=127.0.0.1
FLINK_JOBMANAGER_PORT=8084

LOKI_IP=127.0.0.1
LOKI_PORT=3100
PROMETHEUS_IP=127.0.0.1
PROMETHEUS_PORT=9090
GRAFANA_IP=127.0.0.1
GRAFANA_PORT=3000

KAFKA_UI_PORT=8090
```

## 4. Single-machine 실행 순서

모든 명령은 프로젝트 루트에서 실행합니다.

## 4.1 Kafka

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d
```

검증:

```bash
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env ps
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

## 4.2 ClickHouse

```bash
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up -d
curl -s http://localhost:8123/ping
# expected: Ok
```

## 4.3 Ingestor (3 replicas)

```bash
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up -d --build
```

검증:

```bash
curl -s http://localhost:8081/health
curl -s http://localhost:8082/health
curl -s http://localhost:8083/health
```

## 4.4 Nginx LB

```bash
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up -d
curl -s http://localhost:8080/health
```

## 4.5 Flink

```bash
# JAR build (코드 변경 시마다 필요)
cd services/flink-job && mvn clean package && cd ../..

# JobManager + TaskManager 기동
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up -d --build
```

검증:

```bash
docker logs -f flink-taskmanager-1
docker exec flink-jobmanager /opt/flink/bin/flink list
```

## 4.6 Generator

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
./build/generate config/default.yaml
```

기본 재생속도는 `5x`입니다.

## 4.7 End-to-End 스모크 체크

```bash
# Nginx health
curl -s http://localhost:8080/health

# 최근 Nginx 응답 코드 분포 (단기)
docker logs --since=10s ingestor-lb 2>&1 | awk '{print $9}' | sort | uniq -c

# ClickHouse 증가 확인
C1=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_events")
sleep 5
C2=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM default.taxi_events")
echo "rate_per_sec=$(( (C2-C1)/5 ))"
```

## 4.8 Monitoring Stack (선택)

```bash
# Prometheus + Kafka Exporter
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env up -d

# Grafana
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env up -d
```

검증:

```bash
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
```

## 5. Multi-machine 실행 순서

핵심: `config/.env`는 모든 머신에서 동일한 값이어야 하며, 각 머신은 자기 역할 서비스만 실행합니다.

권장 롤 분리:

- 머신 A: Kafka-1
- 머신 B: Kafka-2
- 머신 C: Kafka-3 + Kafka UI
- 머신 D: ClickHouse
- 머신 E: Ingestor 3개 + Nginx LB
- 머신 F: Flink
- 머신 G: Generator
- 머신 H: Loki + Prometheus + Grafana (같은 머신도 가능하지만 `LOKI_IP`, `PROMETHEUS_IP`, `GRAFANA_IP`는 각각 분리 정의 권장)

## 5.1 Kafka 클러스터

```bash
# machine A
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-1

# machine B
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-2

# machine C
docker compose -f infra/kafka/docker-compose.distributed.yml --env-file config/.env up -d kafka-3 kafka-ui
```

## 5.2 ClickHouse

```bash
# machine D
docker compose -f infra/clickhouse/docker-compose.distributed.yml --env-file config/.env up -d clickhouse
```

## 5.3 Ingestor + Nginx

```bash
# machine E
docker compose -f services/ingestor/docker-compose.distributed.yml --env-file config/.env up -d --build ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.distributed.yml --env-file config/.env up -d nginx-lb
```

## 5.4 Flink

```bash
# machine F
cd services/flink-job && mvn clean package && cd ../..
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d --build flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
```

## 5.5 Generator

```bash
# machine G
cd services/generator
./build/generate
```

## 6. 유의사항 (중요)

1. `config/.env`를 git에 커밋하지 않습니다.
2. 분산 모드에서 방화벽/보안그룹에 아래 포트를 반드시 오픈합니다.
3. Kafka는 브로커별 external/internal/controller 포트가 모두 필요합니다.
4. 분산 Nginx는 템플릿 기반(`envsubst`)이므로 `INGESTOR_*_IP/PORT` 누락 시 기동 실패합니다.
5. Flink 이미지는 JAR를 자동 빌드하지 않습니다. `mvn package` 후 실행해야 합니다.
6. Generator는 compose 서비스가 아니라 native binary입니다.
7. `kafka-network`는 단일 머신 기준으로는 compose가 생성/재사용합니다. 구성 손상 시 `docker network ls | rg kafka-network`로 확인합니다.

## 7. 방화벽/포트 체크리스트 (멀티 머신)

- Kafka: `9092`, `9094`, `9096`, `29092`, `29094`, `29096`, `19093`, `19094`, `19095`
- Ingestor: `8081`, `8082`, `8083`
- Nginx LB: `8080`
- ClickHouse: `8123`, `9000`
- Flink UI: `8084`
- Kafka UI: `8090`

## 8. 상태 확인/로그

```bash
# Kafka
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env ps

# Ingestor
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env ps
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env logs -f ingestor-1 ingestor-2 ingestor-3

# ClickHouse
curl -s http://localhost:8123/ping

# Nginx LB
curl -s http://localhost:${NGINX_LB_PORT:-8080}/health

# Flink
docker logs -f flink-taskmanager-1
docker logs -f flink-taskmanager-2
docker logs -f flink-taskmanager-3
```

## 9. Grafana Geomap 운영값

- GeoJSON 갱신 주기: `900s` (15분)
- GeoJSON 집계 윈도우: 최근 `1440분` (24시간)
- 레이어 기준: `PICKUP` 이벤트 점유율(`demand_pct`)

참고:

- 회색 폴리곤(`No Data (0%)`)은 "해당 윈도우에서 `PICKUP=0`" 의미입니다.
- 동일 zone에 `DROPOFF`/`INTRANSIT` 데이터가 있어도 `PICKUP=0`이면 회색으로 표시됩니다.

## 10. 중지/정리

### 10.1 프로세스 정지(데이터 유지)

```bash
# generator (백그라운드 실행 중인 경우)
pkill -f "generate" || true

docker compose -f infra/flink/docker-compose.yml --env-file config/.env down
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env down
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env down
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env down
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down
```

### 10.2 완전 정리(데이터/볼륨 삭제)

아래 명령은 Kafka/ClickHouse/Grafana 데이터까지 모두 삭제합니다.

```bash
# generator
pkill -f "generate" || true

docker compose -f infra/grafana/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/flink/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env down -v --remove-orphans
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env down -v --remove-orphans
```

### 10.3 ClickHouse만 초기화(컨테이너 유지)

```bash
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_events"
docker exec clickhouse clickhouse-client --query "TRUNCATE TABLE default.taxi_predictions"
```

분산 모드는 각 머신에서 해당 compose 파일로 동일하게 `down`을 실행합니다.

## 11. Optional: S3 Sink Branch

1. Provision S3 + IAM in `infra/terraform`:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

2. Prepare connector config:

```bash
cd ../connectors
cp s3-sink-config.template.json s3-sink-config.json
```

3. Fill bucket and AWS keys in `infra/connectors/s3-sink-config.json`.
4. Start Kafka Connect worker (not included in this repository).
5. Register connector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @s3-sink-config.json \
  http://localhost:8083/connectors
```

## 12. Common Pitfalls

1. Missing `--env-file config/.env` causes unresolved `${VAR}` or wrong defaults.
2. `config/.env` with non-network keys breaks source-of-truth policy.
3. `kafka-network` missing on non-kafka machines breaks ClickHouse/Flink startup.
4. Flink TaskManager machine without local `flink-taxi-job:latest` image fails to start.
5. Single-machine Flink compose uses internal Docker DNS (`kafka-*`, `clickhouse`); only distributed mode should use real reachable host IPs in `config/.env`.
6. 현재 `infra/flink/docker-compose.distributed.yml` 기준 `flink-taskmanager-3`에는 `/opt/flink/model` 마운트가 없어 ONNX 예측 오퍼레이터가 해당 TM에 스케줄되면 실패할 수 있습니다.
