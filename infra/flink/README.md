# Flink Runtime Guide

## 파일 위치
- Single-machine compose: `ops/compose/single-machine/docker-compose.yml`
- Distributed compose: `ops/compose/distributed/docker-compose.yml`
- Job source: `services/flink-job`
- Job config YAML: `services/flink-job/config/default.yaml`

Flink 컨테이너는 `FLINK_CONFIG_PATH=/opt/flink/config/default.yaml`로 YAML을 로딩합니다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## Flink 컨테이너 시작
```bash
# single-machine
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink

# distributed
docker compose -f ops/compose/distributed/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink
```

## Job 빌드/배포
```bash
cd services/flink-job
mvn clean package

# jar 복사 (artifact 이름은 target/ 내 실제 파일 확인)
docker cp target/flink-kafka-print-0.1.0.jar flink-jobmanager:/opt/flink/usrlib/

# 실행
docker exec flink-jobmanager flink run -c com.example.TaxiRealtimeJob /opt/flink/usrlib/flink-kafka-print-0.1.0.jar
```

## 확인
```bash
docker logs -f flink-taskmanager
```
