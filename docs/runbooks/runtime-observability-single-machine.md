# Runtime Runbook - Observability (Single Machine)

이 문서는 단일 머신에서 Loki + Prometheus + Grafana를 단독 기동하거나 장애 복구할 때 사용하는 상세 절차입니다.
기본 from-scratch 흐름에는 `docs/runbooks/runtime-single-machine-from-scratch.md`에 이미 포함되어 있습니다.

## 0. 전제

1. 아래 런타임이 이미 기동되어 있어야 지표/로그가 실제로 수집됩니다.
   - Kafka, ClickHouse, Ingestor, Nginx, Flink, Generator
2. 아직 런타임을 안 올렸다면 먼저 `docs/runbooks/runtime-single-machine-from-scratch.md`를 수행하세요.

## 1. 환경파일 준비

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject
cp config/.env.single-machine config/.env
```

## 2. Loki + Promtail 기동

```bash
docker compose -f infra/loki/docker-compose.yml --env-file config/.env up
```

검증:

```bash
curl -sf http://127.0.0.1:3100/ready
curl -sf "http://127.0.0.1:${PROMTAIL_LOKI_PORT:-9084}/metrics" >/dev/null
```

## 3. Prometheus 기동

```bash
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env up
```

검증:

```bash
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/healthy"
```

## 4. Grafana 기동

```bash
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env up
```

검증:

```bash
curl -sf "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
```

## 5. 수집 상태 점검

### 5.1 Prometheus target 점검

```bash
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/targets" | python3 -c "
import sys, json
items = json.load(sys.stdin)['data']['activeTargets']
for t in items:
    print(f\"{t['labels'].get('job','?'):20s} {t['health']:6s} {t.get('lastError','')}\")
"
```

기대 결과:

- `kafka`, `ingestor`, `nginx`, `flink`, `clickhouse`, `generator` 주요 job이 `up`

### 5.2 대표 metric 확인

```bash
curl -sf "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/query?query=up"
```

### 5.3 Grafana datasource 확인

```bash
curl -sf -u admin:admin "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/datasources"
```

## 6. Grafana 접속

- URL: `http://localhost:3000`
- 기본 계정: `admin / admin`
- Dashboard에서 NYC Taxi 관련 패널 데이터가 들어오는지 확인

## 7. 자주 나는 문제

1. `prometheus`는 떴는데 target이 `down`
   - 런타임 서비스가 실제로 떠 있는지 먼저 확인 (`docker compose ... ps`)
2. Grafana 패널이 `No data`
   - ClickHouse row count 확인: `docker exec clickhouse clickhouse-client -q "SELECT count() FROM default.taxi_events"`
3. Loki 로그가 비어 있음
   - `services/generator/data/generator.log`가 생성되는 실행 방식(`tee`)인지 확인
