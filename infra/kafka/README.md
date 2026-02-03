# Kafka 3-Broker KRaft Cluster

## 구조

각 브로커를 독립적인 docker-compose 파일로 관리합니다.

```
kafka/
├── docker-compose.kafka-1.yml  (Broker 1)
├── docker-compose.kafka-2.yml  (Broker 2)
├── docker-compose.kafka-3.yml  (Broker 3)
├── docker-compose.kafka-ui.yml (UI)
└── docker-compose.yml          (통합 파일 - 옵션)
```

## 실행 방법

### 1. 네트워크 생성 (한 번만)
```bash
docker network create kafka-network
```

### 2. 브로커 시작
```bash
cd infra/kafka

docker compose \
  -f docker-compose.kafka-1.yml \
  -f docker-compose.kafka-2.yml \
  -f docker-compose.kafka-3.yml \
  -f docker-compose.kafka-ui.yml \
  up -d
```

### 3. 상태 확인
```bash
docker compose -f docker-compose.kafka-1.yml ps
docker compose -f docker-compose.kafka-2.yml ps
docker compose -f docker-compose.kafka-3.yml ps
docker compose -f docker-compose.kafka-ui.yml ps
```

또는:
```bash
docker ps | grep kafka
```

## 포트 매핑

| 서비스 | EXTERNAL | INTERNAL | CONTROLLER |
|--------|----------|----------|-----------|
| kafka-1 | 9092 | 29092 | 19093 |
| kafka-2 | 9094 | 29092 | 19094 |
| kafka-3 | 9096 | 29092 | 19095 |
| kafka-ui | 8090 | - | - |

## 토픽 확인
```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic taxi-event-data
```

## 종료

```bash
# 모두 종료
docker compose \
  -f docker-compose.kafka-1.yml \
  -f docker-compose.kafka-2.yml \
  -f docker-compose.kafka-3.yml \
  -f docker-compose.kafka-ui.yml \
  down -v

# 네트워크 제거
docker network rm kafka-network
```

## 개별 브로커 관리

각 브로커는 독립적으로 관리됩니다:

```bash
# Broker 1만 재시작
docker compose -f docker-compose.kafka-1.yml restart

# Broker 2만 로그 확인
docker compose -f docker-compose.kafka-2.yml logs -f

# Broker 3만 제거
docker compose -f docker-compose.kafka-3.yml down
```

## 아키텍처

```
┌────────────────────────────────────────────┐
│        kafka-network (Docker Bridge)       │
├────────────────────────────────────────────┤
│                                            │
│  ┌──────────────┐ ┌──────────────┐       │
│  │  kafka-1     │ │  kafka-2     │       │
│  │ :9092        │ │ :9094        │       │
│  │ :29092       │ │ :29092       │       │
│  │ :19093       │ │ :19094       │       │
│  └──────────────┘ └──────────────┘       │
│         ↓                ↓                │
│         └────────┬───────┘                │
│                  │                        │
│          ┌──────────────┐                 │
│          │  kafka-3     │                 │
│          │ :9096        │                 │
│          │ :29092       │                 │
│          │ :19095       │                 │
│          └──────────────┘                 │
│                  │                        │
│          ┌──────────────┐                 │
│          │  kafka-ui    │                 │
│          │ :8090        │                 │
│          └──────────────┘                 │
└────────────────────────────────────────────┘
```

## KRaft 설정

- **Cluster ID**: MkU3OEVCNTcwNTJENDM2Qk
- **Controller Quorum Voters**: 1@kafka-1:19093,2@kafka-2:19093,3@kafka-3:19093
- **Replication Factor**: 2
- **Min ISR**: 2

## 트러블슈팅

### DNS 해석 오류
```
UnknownHostException: kafka-2
```
→ 네트워크가 제대로 생성되었는지 확인:
```bash
docker network ls | grep kafka
docker network inspect kafka-network
```

### 토픽 RF=1 문제
```bash
# 토픽 삭제 후 재생성
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic taxi-event-data
```
