# Grafana Monitoring Implementation

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                    GRAFANA (v11.0.0:3000)                     │
│              3 Dashboards / 3 Datasources                     │
└──────────┬──────────────────┬──────────────────┬─────────────┘
           │                  │                  │
   ┌───────▼──────┐   ┌──────▼──────┐    ┌──────▼──────┐
   │  ClickHouse  │   │ Prometheus  │    │    Loki     │
   │  :9000       │   │  :9090      │    │   :3100     │
   │  (Taxi Data) │   │  (Metrics)  │    │   (Logs)    │
   └──────────────┘   └──────┬──────┘    └──────┬──────┘
                             │                  │
                    ┌────────▼──────┐    ┌──────▼──────┐
                    │ Kafka Exporter│    │  Promtail   │
                    │  :9308        │    │  (Docker    │
                    │ Blackbox      │    │   Logs)     │
                    │  Exporter     │    └─────────────┘
                    └───────────────┘
```

## Datasources

| Name | Type | Connection | Default |
|---|---|---|---|
| ClickHouse | grafana-clickhouse-datasource | `clickhouse:9000` (native) | Yes |
| Prometheus | prometheus | `http://prometheus:9090` | No |
| Loki | loki | `http://loki:3100` | No |

---

## Dashboard 1: NYC Taxi Events

**UID:** `taxi-events-dashboard` | **Refresh:** 10s | **11 Panels**

### Stat Panels (요약 지표)

| ID | Title | Query |
|---|---|---|
| 1 | Total Events | `SELECT count() FROM default.taxi_events` |
| 3 | Unique Trips | `SELECT uniq(trip_id) FROM default.taxi_events` |
| 2 | Avg Trip Duration | `SELECT round(avg(duration), 1) FROM (SELECT trip_id, dateDiff('minute', minIf(ts, event='PICKUP'), maxIf(ts, event='DROPOFF')) as duration FROM default.taxi_events GROUP BY trip_id HAVING duration > 0 AND duration < 180)` |

### Time Series Panels (실시간 추이)

| ID | Title | Type | Query |
|---|---|---|---|
| 4 | Events Over Time | line | `SELECT toStartOfMinute(ts) as time, event, count() FROM default.taxi_events GROUP BY time, event ORDER BY time` |
| 6 | Events Per Minute | bar | `SELECT toStartOfMinute(ts) as time, count() as events_per_min FROM default.taxi_events GROUP BY time ORDER BY time` |

### Bar Charts (분포)

| ID | Title | Query |
|---|---|---|
| 5 | Top 10 Zones by Demand | `SELECT toString(zone_id) as zone, count() as demand FROM default.taxi_events WHERE event = 'PICKUP' GROUP BY zone_id ORDER BY demand DESC LIMIT 10` |
| 7 | Hourly Event Distribution | `SELECT toString(toHour(ts)) as hour, countIf(event='PICKUP') as PICKUP, countIf(event='DROPOFF') as DROPOFF, countIf(event='IN_TRANSIT') as IN_TRANSIT FROM default.taxi_events GROUP BY hour ORDER BY toHour(ts)` |

### Geomap Panel (Choropleth)

| ID | Title | Description |
|---|---|---|
| 9 | Pickup Demand by Zone | 8단계 GeoJSON choropleth, CartoDB Dark Matter 배경 |

### Kafka Monitoring (Prometheus)

| ID | Title | PromQL |
|---|---|---|
| 10 | Kafka Consumer Lag | `sum(kafka_consumergroup_lag) by (consumergroup, topic)` |
| 11 | Kafka Messages In/sec | `sum(rate(kafka_topic_partition_current_offset{topic="taxi-event-data"}[1m])) by (topic)` |

### Table

| ID | Title | Query |
|---|---|---|
| 8 | Recent Events | `SELECT trip_id, ts, zone_id, event FROM default.taxi_events ORDER BY ts DESC LIMIT 50` |

---

## Dashboard 2: Service Health

**UID:** `service-health-dashboard` | **Refresh:** 15s | **14 Panels**

Blackbox Exporter를 통한 서비스 상태 모니터링.

### Overview Panels

| ID | Title | PromQL |
|---|---|---|
| 1 | Service Health Status | `probe_success{job=~"blackbox.*"}` |
| 2 | Service Uptime History | `probe_success{job=~"blackbox.*"}` (timeseries) |
| 3 | Response Time (HTTP) | `probe_duration_seconds{job="blackbox-http"}` |

### Individual Service Health (stat panels, UP/DOWN)

| ID | Service | Probe Type | Target |
|---|---|---|---|
| 4 | ClickHouse | HTTP | `clickhouse:8123` |
| 5 | Grafana | HTTP | `grafana:3000` |
| 6 | Loki | HTTP | `loki:3100` |
| 7 | Flink | HTTP | `flink-jobmanager:8084` |
| 8 | Nginx LB | HTTP | `nginx:8080` |
| 9 | Kafka 1 | TCP | `kafka1:9092` |
| 10 | Kafka 2 | TCP | `kafka2:9094` |
| 11 | Kafka 3 | TCP | `kafka3:9096` |
| 12 | Ingestor 1 | HTTP | `ingestor1:8081` |
| 13 | Ingestor 2 | HTTP | `ingestor2:8082` |
| 14 | Ingestor 3 | HTTP | `ingestor3:8083` |

---

## Dashboard 3: Pipeline Logs (Loki)

**UID:** `loki-logs-dashboard` | **Refresh:** 10s | **8 Panels**

Promtail → Loki를 통한 Docker 컨테이너 로그 수집/조회.

### Overview

| ID | Title | Type | LogQL |
|---|---|---|---|
| 1 | Log Volume by Container | timeseries (stacked bars) | `sum(count_over_time({job="docker"} [1m])) by (container)` |
| 2 | Error Logs (All Services) | logs | `{job="docker"} \|~ "(?i)(error\|exception\|fail\|fatal)"` |

### Pipeline Service Logs

| ID | Title | LogQL |
|---|---|---|
| 10 | Kafka Broker Logs | `{job="docker", container=~"kafka-[0-9]+"}` |
| 11 | Flink Logs | `{job="docker", container=~"flink-.*"}` |
| 12 | Ingestor Logs | `{job="docker", container=~"ingestor-[0-9]+"}` |
| 13 | Nginx LB Logs | `{job="docker", container="ingestor-lb"}` |

### Infrastructure Logs

| ID | Title | LogQL |
|---|---|---|
| 4 | ClickHouse Logs | `{job="docker", container="clickhouse"}` |
| 3 | All Container Logs | `{job="docker"}` |

### Promtail 수집 대상 컨테이너

**Pipeline:** `kafka-[0-9]+`, `kafka-ui`, `flink-jobmanager`, `flink-taskmanager`, `ingestor-[0-9]+`, `ingestor-lb`
**Infra:** `clickhouse`, `grafana`, `kafka-exporter`, `loki`, `prometheus`, `promtail`

> **Note:** Promtail은 같은 Docker host의 컨테이너 로그만 수집 가능. 원격 머신의 서비스 로그를 수집하려면 해당 머신에도 Promtail을 배포하고 동일한 Loki로 push해야 함.

---

## Choropleth Map 구현

### 방식

Grafana Geomap의 GeoJSON 레이어는 Folium `choropleth`처럼 연속 색상 매핑(`color.field`)을 지원하지 않음. demand 구간별로 GeoJSON을 분리하고, 각각 고정 색상의 레이어로 쌓는 방식 사용.

```
ClickHouse (demand 집계) → Python 스크립트로 GeoJSON 8분할 → Grafana 8개 GeoJSON 레이어
```

### 8단계 색상 팔레트

| 구간 | 색상 | Zone 수 | 설명 |
|---|---|---|---|
| 0 | `#e0e0e0` | 39 | No Data |
| 1-10 | `#d9f0d3` | 51 | Minimal |
| 11-50 | `#a1d99b` | 66 | Low |
| 51-200 | `#fddd7e` | 23 | Medium |
| 201-800 | `#fdae61` | 28 | Mid-High |
| 801-2000 | `#f46d43` | 19 | High |
| 2001-5000 | `#d73027` | 22 | Very High |
| 5000+ | `#a50026` | 15 | Max |

임계값 기준: 실제 데이터 분포 (median=45, P75=830, P90=3942, max=13047)

### GeoJSON 파일 구조

```
provisioning/geojson/
├── taxi_zones.geojson           # 원본 (263 zones, demand 값 포함)
├── taxi_zones_t0_none.geojson   # demand = 0
├── taxi_zones_t1_minimal.geojson # 1-10
├── taxi_zones_t2_low.geojson    # 11-50
├── taxi_zones_t3_medium.geojson # 51-200
├── taxi_zones_t4_mid.geojson    # 201-800
├── taxi_zones_t5_high.geojson   # 801-2000
├── taxi_zones_t6_vhigh.geojson  # 2001-5000
└── taxi_zones_t7_max.geojson    # 5000+
```

### 제외된 Zone (좌표 0,0)

| zone_id | zone_name | 이유 |
|---|---|---|
| 57 | Corona | lat=0, lon=0 |
| 104 | Governor's Island/Ellis Island/Liberty Island | lat=0, lon=0 |
| 105 | Governor's Island/Ellis Island/Liberty Island | lat=0, lon=0 |
| 264 | N/A | lat=0, lon=0 |
| 265 | Outside of NYC | lat=0, lon=0 |

---

## Troubleshooting Log

### 1. 마커가 아프리카 서쪽 바다(0,0)에 찍히는 문제

- **증상**: 미국 전역에 마커가 분산됨
- **원인**: `lat=0, lon=0`인 zone 5개가 Null Island(0,0)에 표시
- **해결**: SQL에 `WHERE z.zone_id NOT IN (57, 104, 105, 264, 265)` 추가

### 2. GeoJSON 레이어 404 에러

- **증상**: 지도는 뜨지만 폴리곤이 안 보임
- **로그**: `path="/[object Object]" status=404`
- **원인**: `src` 설정이 object 형태 `{"mode":"url","url":"..."}` → JS에서 `[object Object]` 문자열로 변환되어 fetch
- **해결**: `src`를 문자열로 변경 `"src": "public/geojson/taxi_zones.geojson"`

### 3. 분할 GeoJSON 파일이 컨테이너에 없는 문제

- **증상**: 새로 만든 GeoJSON 파일이 404
- **원인**: docker-compose에서 단일 파일만 마운트
- **해결**: 디렉토리 전체 마운트로 변경

```yaml
# Before (단일 파일)
- ./provisioning/geojson/taxi_zones.geojson:/usr/share/grafana/public/geojson/taxi_zones.geojson:ro

# After (디렉토리)
- ./provisioning/geojson:/usr/share/grafana/public/geojson:ro
```

### 4. `color.field`로 연속 색상이 안 되는 문제

- **시도**: 단일 GeoJSON + `"color":{"field":"demand"}` + `continuous-GrYlRd`
- **결과**: 툴팁에 demand 값은 표시되나 폴리곤 fill 색상 미적용 (투명)
- **결론**: Grafana 11 Geomap GeoJSON 레이어는 property 기반 fill color 미지원
- **해결**: demand 구간별 GeoJSON 분할 + 각 레이어에 `color.fixed` 적용

### 5. 4단계 색상이 단조로운 문제

- **증상**: 맨해튼 전체가 빨강, 외곽 전체가 초록으로 구분 안 됨
- **원인**: 4개 tier만으로는 극단적 편향 분포(median=45, max=13047)를 표현 못함
- **해결**: 8단계로 세분화, 데이터 분위수(P25/P50/P75/P90) 기반 임계값 설정

---

## Docker Compose 구성

### Grafana (`infra/grafana/docker-compose.yml`)

```yaml
grafana:
  image: grafana/grafana:11.0.0
  ports: "${GRAFANA_PORT:-3000}:3000"
  plugins: grafana-clickhouse-datasource
  volumes:
    - ./provisioning:/etc/grafana/provisioning      # dashboards, datasources
    - ./provisioning/geojson:/usr/share/grafana/public/geojson:ro  # choropleth
```

### Prometheus Stack (`infra/prometheus/docker-compose.yml`)

```yaml
prometheus:       prom/prometheus:v2.51.0      :9090   # 메트릭 수집, 7일 보관
blackbox-exporter: prom/blackbox-exporter:v0.25.0       # HTTP/TCP 헬스체크
kafka-exporter:   danielqsj/kafka-exporter:v1.7.0 :9308 # Kafka consumer lag
```

### Loki Stack (`infra/loki/docker-compose.yml`)

```yaml
loki:     grafana/loki:3.3.2     :3100   # 로그 저장/쿼리 (TSDB, filesystem)
promtail: grafana/promtail:3.3.2         # Docker 컨테이너 로그 수집
```

모든 서비스는 `kafka-network` (external) 네트워크 사용.

---

## Prometheus Scrape Targets

```yaml
scrape_configs:
  - job: prometheus        → localhost:9090
  - job: kafka             → kafka-exporter:9308
  - job: blackbox-http     → 7개 HTTP 엔드포인트 (ClickHouse, Grafana, Loki, Flink, Nginx, Ingestors)
  - job: blackbox-tcp      → 3개 Kafka 브로커 (9092, 9094, 9096)
```

## Promtail 수집 설정

- Docker socket(`/var/run/docker.sock`)으로 컨테이너 자동 발견
- Pipeline: `kafka-[0-9]+`, `kafka-ui`, `flink-jobmanager`, `flink-taskmanager`, `ingestor-[0-9]+`, `ingestor-lb`
- Infra: `clickhouse`, `grafana`, `kafka-exporter`, `loki`, `prometheus`, `promtail`
- 라벨: `job="docker"`, `container=<name>`
