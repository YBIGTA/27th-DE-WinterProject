# Loki Runtime Guide

## 파일 위치
- Compose: infra/loki/docker-compose.yml
- Loki 설정: infra/loki/loki-config.yml
- Promtail 설정: infra/loki/promtail-config.yml

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

#### 2. 각 팀원 머신에서 Promtail 실행
모든 서비스 머신(Ingestor, Kafka, ClickHouse, Flink 등)에서:
```bash
cd infra/loki
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d promtail
```

**동작 방식:**
- 각 머신의 Promtail이 로컬 Docker 컨테이너 로그 수집
- 중앙 Loki(${LOKI_IP}:${LOKI_PORT})로 자동 전송
- 모든 머신의 로그를 한곳에서 조회 가능

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

# 각 머신의 Promtail
docker compose -f docker-compose.distributed.yml down promtail
```
