# Runtime Runbook - Observability (Distributed)

이 문서는 분산 환경에서 Loki + Prometheus + Grafana를 단독 기동하거나 장애 복구할 때 사용하는 상세 절차입니다.
기본 from-scratch 흐름(`docs/runbooks/runtime-distributed-from-scratch.md`)에도 관측 스택 단계가 포함되어 있습니다.
관측 스택은 보통 머신 H(전용 또는 공유)에 올립니다.

## 0. 전제

1. 분산 런타임( Kafka/ClickHouse/Ingestor/Nginx/Flink/Generator )이 이미 동작 중이어야 합니다.
2. 아직 런타임이 없다면 `docs/runbooks/runtime-distributed-from-scratch.md`를 먼저 수행하세요.
3. `config/.env`는 모든 머신에서 동일해야 하며 실제 IP를 가져야 합니다.
   특히 Flink는 `FLINK_IP`(JobManager)와 `FLINK_TASKMANAGER_1_IP/2_IP/3_IP`(각 TaskManager)가 올바르게 설정되어야 합니다.

## 1. 머신 H 공통 준비

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject
```

네트워크 생성:

```bash
docker network create kafka-network || true
docker network ls | rg kafka-network
```

환경파일 준비(실제 분산 IP 반영본):

```bash
cp config/.env.distributed config/.env
# 이후 실제 IP/PORT 반영
```

쉘에서 `${VAR}`를 사용하는 검증 명령을 위해 환경 로드:

```bash
set -a
source config/.env
set +a
```

## 2. Loki + Promtail 기동 (H)

```bash
docker compose -f infra/loki/docker-compose.distributed.yml --env-file config/.env up -d
```

참고:

- 서비스 머신 로그는 각 서비스 compose의 `promtail` sidecar가 Loki로 전송합니다.
- Nginx 로그 기반 메트릭은 E 머신의 `promtail-nginx`(`http://${NGINX_IP}:${NGINX_PROMTAIL_PORT:-9085}/metrics`)에서 노출됩니다.
- H 머신의 `promtail-loki`는 Loki 호스트 로컬 수집/메트릭 노출 용도입니다.

검증:

```bash
curl -sf "http://${LOKI_IP}:${LOKI_PORT}/ready"
curl -sf "http://127.0.0.1:${PROMTAIL_LOKI_PORT:-9084}/metrics" >/dev/null
curl -sf "http://${NGINX_IP}:${NGINX_PROMTAIL_PORT:-9085}/metrics" >/dev/null
```

## 3. Prometheus 기동 (H)

```bash
docker compose -f infra/prometheus/docker-compose.distributed.yml --env-file config/.env up -d
```

검증:

```bash
curl -sf "http://${PROMETHEUS_IP}:${PROMETHEUS_PORT}/-/healthy"
```

## 4. Grafana 기동 (H)

```bash
docker compose -f infra/grafana/docker-compose.distributed.yml --env-file config/.env up -d
```

검증:

```bash
curl -sf "http://${GRAFANA_IP}:${GRAFANA_PORT}/api/health"
```

## 5. 수집/연결 검증

### 5.1 Prometheus targets

```bash
curl -sf "http://${PROMETHEUS_IP}:${PROMETHEUS_PORT}/api/v1/targets" | python3 -c "
import sys, json
items = json.load(sys.stdin)['data']['activeTargets']
for t in items:
    job = t['labels'].get('job','?')
    health = t['health']
    err = t.get('lastError','')
    print(f\"{job:20s} {health:6s} {err}\")
"
```

기대 결과:

- `kafka`, `kafka-jmx`, `ingestor`, `nginx`, `promtail-nginx`, `flink`, `clickhouse`, `generator`, `promtail-loki` job이 `up`

### 5.2 Grafana datasource

```bash
curl -sf -u admin:admin "http://${GRAFANA_IP}:${GRAFANA_PORT}/api/datasources"
```

기대 결과:

- ClickHouse / Prometheus / Loki datasource가 존재

## 6. 분산 환경에서 흔한 오류

1. target 대부분이 `down`
   - `config/.env`의 `*_IP`가 `127.0.0.1`로 남아있는지 확인
2. Kafka 관련 target만 `down`
   - Kafka 머신 방화벽에서 `9092/9094/9096` 및 JMX exporter 포트(`9404/9405/9406`) 확인
3. Flink target만 `down`
   - `FLINK_IP`, `FLINK_TASKMANAGER_1_IP`, `FLINK_TASKMANAGER_2_IP`, `FLINK_TASKMANAGER_3_IP` 값 확인
   - `FLINK_JOBMANAGER_METRICS_PORT`, `FLINK_TASKMANAGER_*_METRICS_PORT` 포트 오픈 여부 확인
4. Grafana에서 Loki 쿼리가 비어 있음
   - promtail가 각 컴포넌트 호스트에서 로그를 읽을 수 있는지(도커 소켓/로그 경로) 확인
5. `promtail-nginx`만 `down`
   - E 머신에서 `NGINX_PROMTAIL_PORT`(기본 `9085`) 포트 오픈 여부 확인
