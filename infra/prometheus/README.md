# Prometheus Runtime Guide

## 파일 위치
- Single-machine Compose: infra/prometheus/docker-compose.yml
- Distributed Compose: infra/prometheus/docker-compose.distributed.yml
- Single-machine 설정: infra/prometheus/prometheus.yml
- Distributed 설정: infra/prometheus/prometheus.distributed.yml

## 역할
- Prometheus: 메트릭 수집 및 저장
- Kafka Exporter: Kafka 브로커 메트릭 노출

## 실행

### Single-machine 모드
```bash
cd infra/prometheus
docker compose up -d
```

### Distributed 모드

중앙 Prometheus 서버 실행 (모니터링 담당 머신):
```bash
cd infra/prometheus
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env up -d
```

**수집 대상:**
- Kafka (${KAFKA_1_IP}:9308) - Kafka Exporter 메트릭
- Ingestor (${INGESTOR_1_IP}:8081/8082, ${INGESTOR_3_IP}:8083) - Actuator 메트릭
- ClickHouse (${CLICKHOUSE_IP}:9363) - ClickHouse 메트릭
- Nginx (${NGINX_IP}:${NGINX_LB_PORT}) - Nginx 메트릭
- Flink (${FLINK_IP}:${FLINK_JOBMANAGER_PORT}) - Flink JobManager 메트릭

## 상태 확인

```bash
# Prometheus 헬스체크
curl http://localhost:9090/-/healthy

# 타겟 상태 확인
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job, health, lastError}'

# Prometheus UI
open http://localhost:9090
```

## 기본 쿼리

### Kafka 메트릭
```promql
# Kafka 토픽별 오프셋
kafka_topic_partition_current_offset{topic="taxi-event-data"}

# 메시지 유입률
rate(kafka_topic_partition_current_offset{topic="taxi-event-data"}[1m])
```

### Ingestor 메트릭 (Actuator 설정 필요)
```promql
# JVM 메모리 사용량
jvm_memory_used_bytes{job="ingestor"}

# HTTP 요청률
rate(http_server_requests_seconds_count{job="ingestor"}[1m])
```

## Grafana 연동

Grafana 데이터소스 설정:
- Type: Prometheus
- URL: http://prometheus:9090 (single-machine) 또는 http://${PROMETHEUS_IP}:${PROMETHEUS_PORT} (distributed)

## 중지

### Single-machine
```bash
docker compose -f infra/prometheus/docker-compose.yml down
```

### Distributed
```bash
docker compose -f docker-compose.distributed.yml --env-file ../../config/.env down
```

## 문제 해결

### 타겟이 DOWN 상태인 경우
1. 타겟 서비스가 실행 중인지 확인
2. 방화벽/네트워크 설정 확인
3. Actuator endpoint 활성화 확인 (Ingestor)
   ```gradle
   implementation 'org.springframework.boot:spring-boot-starter-actuator'
   ```

### 메트릭이 수집되지 않는 경우
```bash
# Prometheus 로그 확인
docker logs prometheus --tail 50

# 설정 파일 검증
docker exec prometheus promtool check config /etc/prometheus/prometheus.yml
```
