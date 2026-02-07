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

## Job 빌드 & 실행 (Application Mode)

Job JAR을 이미지에 포함시켜 `docker compose up` 시 자동으로 Job이 시작된다.

```bash
# 1. JAR 빌드 (최초 또는 코드 변경 시)
cd services/flink-job && mvn clean package && cd ../..

# 2. single-machine — 이미지 빌드 + 컨테이너 시작 (Job 자동 실행)
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up --build

# distributed
docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env up --build -d flink-jobmanager flink
```

## 확인
```bash
docker logs -f flink-taskmanager
```
