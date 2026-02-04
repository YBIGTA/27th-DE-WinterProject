# Flink Runtime Guide

## 파일 위치
- Single-machine compose: `infra/flink/docker-compose.yml`
- Distributed compose: `infra/flink/docker-compose.distributed.yml`
- Job source: `services/flink-job`

Flink job runtime 값(parallelism/topic/jdbc/table 등)은 compose의 `FLINK_*` 환경변수에서 로딩한다.

## 실행 전 준비
```bash
cp config/.env.single-machine config/.env
# distributed는 .env.distributed를 복사 후 실제 IP/PORT 반영
```

## Flink 컨테이너 시작
```bash
# single-machine
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink

# distributed
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up -d flink-jobmanager flink
```

## Job 빌드/배포
```bash
cd services/flink-job
mvn clean package

docker cp target/flink-kafka-print-0.1.0.jar flink-jobmanager:/opt/flink/usrlib/
docker exec flink-jobmanager flink run -c com.example.TaxiRealtimeJob /opt/flink/usrlib/flink-kafka-print-0.1.0.jar
```

## 확인
```bash
docker logs -f flink-taskmanager
```
