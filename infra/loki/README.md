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
```bash
docker compose -f infra/loki/docker-compose.yml up -d
```

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

## 중지
```bash
docker compose -f infra/loki/docker-compose.yml down
```
