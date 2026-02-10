# Monitoring & Dashboard

Grafana + Prometheus 기반 모니터링/대시보드 스택.

## 구성 요소

| 서비스 | 이미지 | 포트 | 역할 |
|--------|--------|------|------|
| Grafana | grafana/grafana:11.0.0 | 3000 | 대시보드 |
| Prometheus | prom/prometheus:v2.51.0 | 9090 | 메트릭 수집 |
| Kafka Exporter | danielqsj/kafka-exporter:v1.7.0 | 9308 | Kafka 메트릭 |

## 실행 방법

```bash
# 사전 조건: kafka-network 생성, ClickHouse 실행 상태

# 1. Prometheus + Kafka Exporter
docker compose -f infra/prometheus/docker-compose.yml --env-file config/.env up -d

# 2. Grafana
docker compose -f infra/grafana/docker-compose.yml --env-file config/.env up -d
```

접속: `http://localhost:3000` (admin / admin)

## 데이터소스

| 이름 | 타입 | 연결 대상 |
|------|------|-----------|
| ClickHouse | grafana-clickhouse-datasource | clickhouse:9000 (native) |
| Prometheus | prometheus | prometheus:9090 |

provisioning으로 자동 등록됨 (`provisioning/datasources/clickhouse.yml`).

## 대시보드: NYC Taxi Events

`provisioning/dashboards/taxi-events.json`으로 자동 프로비저닝.

### 패널 구성

| # | 패널 | 타입 | 데이터소스 | 설명 |
|---|------|------|-----------|------|
| 1 | Total Events | stat | ClickHouse | 전체 이벤트 수 |
| 2 | Events by Type | piechart | ClickHouse | PICKUP/IN_TRANSIT/DROPOFF 비율 |
| 3 | Unique Trips | stat | ClickHouse | 고유 trip 수 |
| 4 | Events Over Time | timeseries | ClickHouse | 시간대별 이벤트 타입 추이 |
| 5 | Top 10 Zones | barchart | ClickHouse | PICKUP 기준 상위 zone |
| 6 | Events Per Minute | timeseries | ClickHouse | 분당 처리량 |
| 7 | Hourly Distribution | barchart | ClickHouse | 시간대별 분포 |
| 8 | Recent Events | table | ClickHouse | 최근 50건 |
| 9 | Pickup Demand Heatmap | geomap | ClickHouse | NYC 지도 위 수요 히트맵 |
| 10 | Kafka Consumer Lag | timeseries | Prometheus | consumer group lag |
| 11 | Kafka Messages In/sec | timeseries | Prometheus | topic 처리량 |

### 사용 쿼리 컬럼

ClickHouse `taxi_events` 테이블 4개 컬럼만 사용:

```
trip_id (UInt64) - trip 식별
ts (DateTime) - 이벤트 시각
zone_id (UInt32) - taxi zone ID
event (String) - PICKUP / IN_TRANSIT / DROPOFF
```

Geomap 히트맵은 `taxi_zones` 테이블과 JOIN:

```
zone_id (UInt32) - PK
zone_name (String) - zone 이름
borough (String) - 자치구
lat (Float64) - 위도
lon (Float64) - 경도
```

## 파일 구조

```
infra/grafana/
├── docker-compose.yml
├── README.md
└── provisioning/
    ├── datasources/
    │   └── clickhouse.yml          # ClickHouse + Prometheus 자동 등록
    └── dashboards/
        ├── dashboard.yml           # dashboard provider 설정
        └── taxi-events.json        # NYC Taxi Events 대시보드

infra/prometheus/
├── docker-compose.yml              # Prometheus + Kafka Exporter
└── prometheus.yml                  # scrape config
```

## 구현 상태

- [x] Grafana 컨테이너 + docker-compose
- [x] ClickHouse datasource 자동 프로비저닝
- [x] Prometheus datasource 자동 프로비저닝
- [x] Prometheus + Kafka Exporter 셋업
- [x] NYC Taxi Events 대시보드 (11개 패널)
- [x] Geomap 히트맵 (taxi_zones JOIN)
- [x] Kafka 메트릭 패널 (Consumer Lag, Messages/sec)
- [x] taxi_zones 테이블 스키마 추가
- [x] .env.single-machine 포트 등록
- [ ] GeoJSON zone 폴리곤 시각화 (shapefile 변환 필요)
- [ ] Flink Prometheus exporter 연동
- [ ] ClickHouse 스키마 확장 (fare, distance 등)
