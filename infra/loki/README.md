# Loki Runtime Guide

## 파일 위치
- Compose: infra/loki/docker-compose.yml
- Loki 설정: infra/loki/loki-config.yml
- Promtail 설정 (single-machine): infra/loki/promtail-config.yml
- Promtail 설정 (distributed): infra/loki/promtail-config.distributed.yml

## 역할
- Loki: 로그 저장/조회
- Promtail: Docker 컨테이너 로그 수집

## 실행 전 준비
- Grafana가 `kafka-network`에 붙어 있으므로 Loki도 동일 네트워크 사용
- Docker 로그 수집을 위해 `/var/lib/docker` read-only 마운트 필요

## 실행

### Single-machine 모드
```bash
docker compose -f infra/loki/docker-compose.yml up -d
```

### Distributed 모드

#### 1. 중앙 Loki 서버 실행 (LOKI_IP 머신)
```bash
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d loki
```

#### 2. 서비스 머신 로그 수집 방식 (권장)
서비스 머신에서는 각 distributed compose 내부의 `promtail` sidecar를 사용합니다.

- Kafka: `infra/kafka/docker-compose.distributed.yml`
- Ingestor: `services/ingestor/docker-compose.distributed.yml`
- ClickHouse: `infra/clickhouse/docker-compose.distributed.yml`
- Nginx: `infra/nginx/docker-compose.distributed.yml`
- Flink: `infra/flink/docker-compose.distributed.yml`

즉, 서비스 기동 시 해당 compose의 promtail이 함께 올라오고, 중앙 Loki(`${LOKI_IP}:${LOKI_PORT}`)로 로그를 전송합니다.

#### 3. `infra/loki`의 promtail 사용 범위
`infra/loki/docker-compose.distributed.yml`의 `promtail`(`promtail-loki`)은 `promtail-config.distributed.yml`을 사용하며 Loki 호스트(H)의 loki 컨테이너/로컬 메트릭 노출 용도로 동작합니다.

분산 서비스 로그는 각 컴포넌트 sidecar promtail이 담당합니다.

## 상태 확인
```bash
# Loki readiness
curl http://localhost:3100/ready

# Promtail 로그
docker logs promtail | tail -20
```

## Grafana 연동
Grafana 데이터소스에 Loki 추가:
- URL: http://loki:3100
- Type: Loki

## 기본 쿼리
```logql
{job="docker"}
```

특정 컨테이너만 조회:
```logql
{job="docker", container="ingestor-1"}
```

## 로그 확인

### 모든 컨테이너 로그
```bash
curl -G http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query={job="docker"}' | jq .
```

### 특정 머신의 로그
Promtail에서 자동으로 `instance` 라벨 추가:
```logql
{job="docker", instance=~"${INGESTOR_1_IP}.*"}
```

## 중지

### Single-machine
```bash
docker compose -f infra/loki/docker-compose.yml down
```

### Distributed
```bash
# 중앙 Loki
docker compose -f docker-compose.distributed.yml down loki

# Loki 호스트의 promtail
docker compose -f docker-compose.distributed.yml down promtail
```
