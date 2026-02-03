# Ingestor 시스템 명세서

## 1. 시스템 개요

Ingestor는 **실시간 택시 이벤트 데이터를 수집하여 Kafka로 전송**하는 고성능 수집 서버입니다.

### 핵심 목표
- **고처리량**: 초당 수만 건의 이벤트 처리
- **고가용성**: 다중 인스턴스를 통한 무중단 서비스
- **배압 관리**: 과부하 상황에서도 안정적인 동작

---

## 2. 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ingestor Cluster                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Nginx Load Balancer                      │  │
│  │                    (포트: 8080)                            │  │
│  │                   least_conn 방식                          │  │
│  └─────────────┬─────────────┬─────────────┬─────────────────┘  │
│                │             │             │                    │
│                ▼             ▼             ▼                    │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐         │
│  │  Ingestor-1   │ │  Ingestor-2   │ │  Ingestor-3   │         │
│  │  (포트:8081)   │ │  (포트:8082)   │ │  (포트:8083)   │         │
│  │               │ │               │ │               │         │
│  │ ┌───────────┐ │ │ ┌───────────┐ │ │ ┌───────────┐ │         │
│  │ │  Buffer   │ │ │ │  Buffer   │ │ │ │  Buffer   │ │         │
│  │ │ (10,000)  │ │ │ │ (10,000)  │ │ │ │ (10,000)  │ │         │
│  │ └─────┬─────┘ │ │ └─────┬─────┘ │ │ └─────┬─────┘ │         │
│  │       │       │ │       │       │ │       │       │         │
│  │   Batching    │ │   Batching    │ │   Batching    │         │
│  │  (500/50ms)   │ │  (500/50ms)   │ │  (500/50ms)   │         │
│  └───────┬───────┘ └───────┬───────┘ └───────┬───────┘         │
│          │                 │                 │                  │
└──────────┼─────────────────┼─────────────────┼──────────────────┘
           │                 │                 │
           └────────────────┬┴─────────────────┘
                            ▼
                 ┌─────────────────────┐
                 │   Kafka Cluster     │
                 │  (taxi-event-data)  │
                 └─────────────────────┘
```

---

## 3. 기술 스택

| 구성 요소 | 기술 | 버전 |
|----------|------|------|
| 프레임워크 | Spring Boot WebFlux | 3.2.1 |
| 런타임 | Netty (비동기 논블로킹) | - |
| 메시지 브로커 | Reactor Kafka | 1.3.22 |
| 로드밸런서 | Nginx | Alpine |
| 언어 | Java | 17 |
| 빌드 | Gradle | 8.5 |

---

## 4. 핵심 컴포넌트 상세

### 4.1 Nginx Load Balancer

**역할**: 클라이언트 요청을 3개의 Ingestor 인스턴스에 분산

```nginx
upstream ingestors {
    least_conn;           # 최소 연결 알고리즘
    server ingestor-1:8080;
    server ingestor-2:8080;
    server ingestor-3:8080;
}
```

**설정 특징**:
| 설정 | 값 | 설명 |
|------|-----|------|
| `least_conn` | - | 현재 연결 수가 가장 적은 서버로 라우팅 |
| `worker_connections` | 4096 | 워커당 최대 동시 연결 수 |
| `proxy_buffering` | off | 스트리밍 최적화 (버퍼링 비활성화) |
| `proxy_connect_timeout` | 5s | 업스트림 연결 타임아웃 |
| `proxy_read_timeout` | 10s | 응답 대기 타임아웃 |

**왜 least_conn인가?**
- 각 Ingestor의 처리 시간이 다를 수 있음
- 느린 서버에 요청이 쌓이는 것을 방지
- Round-robin보다 부하 분산이 균일함

---

### 4.2 Ingestion Controller

**역할**: HTTP 요청을 받아 Service 계층으로 전달

```java
@PostMapping("/ingest")
public Mono<ResponseEntity<Void>> ingest(@RequestBody TaxiEvent event)
```

**응답 코드**:
| HTTP 상태 | 의미 | 클라이언트 행동 |
|----------|------|----------------|
| `202 Accepted` | 이벤트가 버퍼에 추가됨 | 성공 |
| `429 Too Many Requests` | 버퍼 가득 참 (배압) | 잠시 후 재시도 |
| `503 Service Unavailable` | 동시성 문제 | 재시도 |
| `500 Internal Server Error` | 내부 오류 | 로그 확인 필요 |

**엔드포인트**:
- `POST /ingest` - 이벤트 수집
- `GET /health` - 헬스체크 (컨테이너 오케스트레이션용)

---

### 4.3 Ingestion Service (핵심 로직)

**역할**: 이벤트 버퍼링, 배칭, Kafka 전송

#### 4.3.1 데이터 흐름

```
HTTP 요청 수신
      │
      ▼
┌─────────────────────────────────────┐
│         Sinks.Many Buffer           │
│   (최대 10,000개, FAIL_FAST 정책)    │
└─────────────────┬───────────────────┘
                  │
      bufferTimeout(500개 또는 50ms)
                  │
                  ▼
┌─────────────────────────────────────┐
│         Batch Processing            │
│      (4개 병렬 Kafka 전송)           │
└─────────────────┬───────────────────┘
                  │
         Retry (3회, exponential)
                  │
                  ▼
            Kafka Topic
```

#### 4.3.2 버퍼 설정

```java
private final Sinks.Many<TaxiEvent> sink = Sinks.many()
    .multicast()
    .onBackpressureBuffer(10000, false);
```

| 설정 | 값 | 설명 |
|------|-----|------|
| 버퍼 크기 | 10,000 | 최대 대기 이벤트 수 |
| 배압 정책 | FAIL_FAST | 버퍼 초과 시 즉시 거부 (429 반환) |
| multicast | - | 여러 subscriber 지원 |

#### 4.3.3 배칭 전략

```java
sink.asFlux()
    .bufferTimeout(500, Duration.ofMillis(50))
    .flatMap(this::sendBatchToKafkaReactive, 4)
```

| 설정 | 값 | 설명 |
|------|-----|------|
| 배치 크기 | 500개 | 최대 배치당 이벤트 수 |
| 타임아웃 | 50ms | 500개 미만이어도 50ms 후 전송 |
| 병렬 처리 | 4 | 동시 Kafka 전송 수 |

**배칭의 이점**:
- 네트워크 오버헤드 감소 (개별 전송 대비)
- Kafka Producer 효율 극대화
- 처리량 향상

#### 4.3.4 재시도 로직

```java
.retryWhen(Retry.backoff(3, Duration.ofMillis(100))
    .maxBackoff(Duration.ofSeconds(2)))
```

| 설정 | 값 | 설명 |
|------|-----|------|
| 최대 재시도 | 3회 | - |
| 초기 대기 | 100ms | 첫 번째 재시도 전 대기 |
| 최대 대기 | 2초 | exponential backoff 상한 |

---

### 4.4 Kafka Producer 설정

```java
props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);      // 32KB
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
props.put(ProducerConfig.ACKS_CONFIG, "1");
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
```

| 설정 | 값 | 설명 |
|------|-----|------|
| `linger.ms` | 50ms | 배치 수집 대기 시간 |
| `batch.size` | 32KB | 배치 최대 크기 |
| `compression.type` | lz4 | 빠른 압축 알고리즘 |
| `acks` | 1 | 리더만 확인 (성능 우선) |
| `buffer.memory` | 64MB | 프로듀서 버퍼 크기 |
| `max.in.flight` | 5 | 동시 미확인 요청 수 |

---

## 5. Ingestor → Kafka 데이터 흐름 상세

이 섹션에서는 Ingestor Cluster에서 Kafka Cluster로 데이터가 어떻게 이동하는지 상세하게 설명합니다.

### 5.1 전체 흐름도

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTOR CLUSTER                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         Ingestor Instance (x3)                          │   │
│  │                                                                         │   │
│  │   ① HTTP Request                                                        │   │
│  │   POST /ingest { "trip_id": 123, "event": "PICKUP", ... }              │   │
│  │          │                                                              │   │
│  │          ▼                                                              │   │
│  │   ┌─────────────────┐                                                   │   │
│  │   │  Controller     │  TaxiEvent 객체로 역직렬화 (Jackson)               │   │
│  │   └────────┬────────┘                                                   │   │
│  │            │                                                            │   │
│  │            ▼                                                            │   │
│  │   ② Sinks.Many Buffer (10,000개)                                        │   │
│  │   ┌─────────────────────────────────────────────────┐                   │   │
│  │   │ [Event] [Event] [Event] ... [Event] [Event]     │                   │   │
│  │   │  ← 새 이벤트 추가                    대기 중 →   │                   │   │
│  │   └─────────────────────┬───────────────────────────┘                   │   │
│  │                         │                                               │   │
│  │            bufferTimeout(500개 or 50ms)                                 │   │
│  │                         │                                               │   │
│  │                         ▼                                               │   │
│  │   ③ Batch 생성                                                          │   │
│  │   ┌─────────────────────────────────────────────────┐                   │   │
│  │   │ List<TaxiEvent> batch = [Event1, Event2, ...]   │                   │   │
│  │   │ size: 최대 500개                                 │                   │   │
│  │   └─────────────────────┬───────────────────────────┘                   │   │
│  │                         │                                               │   │
│  │                         ▼                                               │   │
│  │   ④ 직렬화 & ProducerRecord 생성                                        │   │
│  │   ┌─────────────────────────────────────────────────┐                   │   │
│  │   │ for each event in batch:                        │                   │   │
│  │   │   key   = event.tripId (String)                 │                   │   │
│  │   │   value = objectMapper.writeValueAsString(event)│                   │   │
│  │   │   → ProducerRecord<String, String>              │                   │   │
│  │   └─────────────────────┬───────────────────────────┘                   │   │
│  │                         │                                               │   │
│  └─────────────────────────┼───────────────────────────────────────────────┘   │
│                            │                                                   │
└────────────────────────────┼───────────────────────────────────────────────────┘
                             │
                             │  TCP Connection (kafka:29092)
                             │  Docker Network: kafka_kafka-network
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA CLUSTER                                      │
│                                                                                 │
│   ⑤ Kafka Producer 내부 처리                                                    │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                     Kafka Producer Buffer (64MB)                        │   │
│   │                                                                         │   │
│   │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│   │   │ Partition 0 │  │ Partition 1 │  │ Partition 2 │  │ Partition 3 │   │   │
│   │   │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │   │   │
│   │   │ │ Batch   │ │  │ │ Batch   │ │  │ │ Batch   │ │  │ │ Batch   │ │   │   │
│   │   │ │ (32KB)  │ │  │ │ (32KB)  │ │  │ │ (32KB)  │ │  │ │ (32KB)  │ │   │   │
│   │   │ └─────────┘ │  │ └─────────┘ │  │ └─────────┘ │  │ └─────────┘ │   │   │
│   │   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │   │
│   │          │                │                │                │          │   │
│   │          └────────────────┴────────────────┴────────────────┘          │   │
│   │                                    │                                    │   │
│   │                         linger.ms (50ms) 대기 후                        │   │
│   │                         또는 batch.size (32KB) 도달 시                  │   │
│   │                                    │                                    │   │
│   │                                    ▼                                    │   │
│   │                           LZ4 압축 적용                                  │   │
│   │                                    │                                    │   │
│   └────────────────────────────────────┼────────────────────────────────────┘   │
│                                        │                                        │
│   ⑥ Kafka Broker 저장                                                          │
│   ┌────────────────────────────────────┼────────────────────────────────────┐   │
│   │           Topic: taxi-event-data   │                                    │   │
│   │                                    ▼                                    │   │
│   │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│   │   │ Partition 0 │  │ Partition 1 │  │ Partition 2 │  │ Partition 3 │   │   │
│   │   │             │  │             │  │             │  │             │   │   │
│   │   │ offset: 0   │  │ offset: 0   │  │ offset: 0   │  │ offset: 0   │   │   │
│   │   │ offset: 1   │  │ offset: 1   │  │ offset: 1   │  │ offset: 1   │   │   │
│   │   │ offset: 2   │  │ offset: 2   │  │ offset: 2   │  │ offset: 2   │   │   │
│   │   │ ...         │  │ ...         │  │ ...         │  │ ...         │   │   │
│   │   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│   │                                                                         │   │
│   │   partition = hash(trip_id) % 4                                         │   │
│   │   → 같은 trip_id는 항상 같은 파티션으로 (순서 보장)                       │   │
│   │                                                                         │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│   ⑦ ACK 응답 (acks=1)                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │   Leader가 로그에 기록 완료 → ACK 전송 → Ingestor 수신                    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 단계별 상세 설명

#### ① HTTP 요청 수신
```
Client → Nginx (8080) → Ingestor (8081/8082/8083)
```
- JSON 형식의 TaxiEvent를 `POST /ingest`로 전송
- Jackson이 자동으로 `@RequestBody TaxiEvent`로 역직렬화

#### ② 버퍼 추가 (Sinks.Many)
```java
Sinks.EmitResult result = sink.tryEmitNext(event);
```
- **성공 시**: `EmitResult.OK` → HTTP 202 반환
- **버퍼 가득 참**: `EmitResult.FAIL_OVERFLOW` → HTTP 429 반환
- 논블로킹으로 즉시 반환 (클라이언트 대기 없음)

#### ③ 배치 생성 (bufferTimeout)
```java
.bufferTimeout(500, Duration.ofMillis(50))
```
- **조건 1**: 500개 이벤트 수집 시 즉시 배치 생성
- **조건 2**: 50ms 경과 시 현재까지 수집된 이벤트로 배치 생성
- 둘 중 먼저 발생하는 조건 적용

#### ④ 직렬화 & ProducerRecord 생성
```java
String key = String.valueOf(event.getTripId());
String value = objectMapper.writeValueAsString(event);
ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
```

| 필드 | 값 | 용도 |
|------|-----|------|
| Topic | `taxi-event-data` | 목적지 토픽 |
| Key | `trip_id` | 파티션 결정 (같은 trip → 같은 파티션) |
| Value | JSON 문자열 | 이벤트 데이터 |

#### ⑤ Kafka Producer 내부 처리
```
Records → Partitioner → Per-Partition Buffer → Batch → Compress → Send
```

| 설정 | 값 | 설명 |
|------|-----|------|
| `buffer.memory` | 64MB | 전체 버퍼 메모리 |
| `batch.size` | 32KB | 파티션당 배치 크기 |
| `linger.ms` | 50ms | 배치 대기 시간 |
| `compression.type` | lz4 | 압축 알고리즘 |
| `max.in.flight` | 5 | 동시 전송 요청 수 |

#### ⑥ Kafka Broker 저장

**파티셔닝 전략**:
```
partition = murmur2(key.getBytes()) % num_partitions
partition = murmur2("123".getBytes()) % 4  // trip_id=123
```
- 같은 `trip_id`는 항상 같은 파티션에 저장
- PICKUP → IN_TRANSIT → DROPOFF 순서 보장

**저장 형식**:
```
┌─────────────────────────────────────────────────┐
│ Offset │ Timestamp │ Key  │ Value (JSON)        │
├─────────────────────────────────────────────────┤
│ 0      │ 170912... │ "1"  │ {"event":"PICKUP"...│
│ 1      │ 170912... │ "1"  │ {"event":"IN_TRAN...│
│ 2      │ 170912... │ "1"  │ {"event":"DROPOFF...│
└─────────────────────────────────────────────────┘
```

#### ⑦ ACK 응답

| acks 설정 | 동작 | 지연 시간 | 내구성 |
|-----------|------|----------|--------|
| `0` | 전송 후 확인 안함 | 최소 | 낮음 |
| **`1` (현재)** | 리더 확인 후 응답 | 중간 | 중간 |
| `all` | 모든 복제본 확인 | 최대 | 높음 |

### 5.3 네트워크 구성

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Network                               │
│                  (kafka_kafka-network)                          │
│                                                                 │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│   │ ingestor-1  │     │ ingestor-2  │     │ ingestor-3  │      │
│   │             │     │             │     │             │      │
│   │ kafka:29092 ├─────┤ kafka:29092 ├─────┤ kafka:29092 │      │
│   └─────────────┘     └─────────────┘     └─────────────┘      │
│          │                   │                   │              │
│          └───────────────────┼───────────────────┘              │
│                              │                                  │
│                              ▼                                  │
│                    ┌─────────────────┐                          │
│                    │      kafka      │                          │
│                    │                 │                          │
│                    │ INTERNAL:29092  │ ← Docker 내부 통신       │
│                    │ EXTERNAL:9092   │ ← 호스트에서 접근        │
│                    └─────────────────┘                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Kafka 리스너 설정**:
| 리스너 | 포트 | 용도 |
|--------|------|------|
| `INTERNAL` | 29092 | Docker 컨테이너 간 통신 (Ingestor → Kafka) |
| `EXTERNAL` | 9092 | 호스트 머신에서 접근 (로컬 개발, 디버깅) |
| `CONTROLLER` | 9093 | KRaft 컨트롤러 통신 |

### 5.4 시퀀스 다이어그램

```
Generator          Nginx LB         Ingestor-1        Kafka Broker
    │                  │                  │                  │
    │  POST /ingest    │                  │                  │
    │─────────────────▶│                  │                  │
    │                  │  forward (least_conn)              │
    │                  │─────────────────▶│                  │
    │                  │                  │                  │
    │                  │                  │ sink.emit(event) │
    │                  │                  │────────┐         │
    │                  │                  │        │ buffer  │
    │                  │                  │◀───────┘         │
    │                  │                  │                  │
    │                  │  202 Accepted    │                  │
    │                  │◀─────────────────│                  │
    │  202 Accepted    │                  │                  │
    │◀─────────────────│                  │                  │
    │                  │                  │                  │
    │                  │                  │  [500개 또는 50ms 후]
    │                  │                  │                  │
    │                  │                  │ kafkaSender.send │
    │                  │                  │─────────────────▶│
    │                  │                  │                  │ write to
    │                  │                  │                  │ partition
    │                  │                  │                  │────┐
    │                  │                  │                  │    │
    │                  │                  │                  │◀───┘
    │                  │                  │    ACK (acks=1)  │
    │                  │                  │◀─────────────────│
    │                  │                  │                  │
    │                  │                  │ eventsProcessed++│
    │                  │                  │────────┐         │
    │                  │                  │◀───────┘         │
    │                  │                  │                  │
```

### 5.5 장애 처리 흐름

```
                    정상 흐름                          장애 발생 시
                        │                                  │
                        ▼                                  ▼
              ┌─────────────────┐                ┌─────────────────┐
              │  Kafka 전송 시도 │                │  Kafka 전송 실패 │
              └────────┬────────┘                └────────┬────────┘
                       │                                  │
                       ▼                                  ▼
              ┌─────────────────┐                ┌─────────────────┐
              │   ACK 수신      │                │  Retry #1       │
              │   성공 처리     │                │  (100ms 대기)   │
              └─────────────────┘                └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  실패 시        │
                                                │  Retry #2       │
                                                │  (200ms 대기)   │
                                                └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  실패 시        │
                                                │  Retry #3       │
                                                │  (400ms 대기)   │
                                                └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  최종 실패      │
                                                │  eventsFailed++ │
                                                │  로그 기록      │
                                                └─────────────────┘
```

---

## 6. 데이터 모델

### TaxiEvent DTO

```java
@Data
public class TaxiEvent {
    private String event;           // PICKUP, IN_TRANSIT, DROPOFF
    private Long tripId;            // 여행 고유 ID (Kafka Key)
    private String timestamp;       // 이벤트 발생 시간

    // 위치 정보
    private Double lat;
    private Double lon;
    private Long puLocationId;      // 픽업 위치 ID
    private Long doLocationId;      // 하차 위치 ID

    // 여행 정보
    private Long vendorId;
    private Long passengerCount;
    private Double tripDistance;

    // 요금 정보 (DROPOFF 시)
    private Double fareAmount;
    private Double totalAmount;
    private Double tipAmount;
    // ... 기타 요금 관련 필드
}
```

**이벤트 타입**:
| 타입 | 설명 | 포함 필드 |
|------|------|----------|
| `PICKUP` | 승객 탑승 | 위치, 승객 수, 요금 정보 시작 |
| `IN_TRANSIT` | 이동 중 | 실시간 위치 (lat, lon) |
| `DROPOFF` | 하차 완료 | 최종 위치, 요금 정산 정보 |

---

## 6. 모니터링

### 메트릭 (10초 주기 로깅)

```
[METRICS] events_received=15000, events_processed=14950,
          events_failed=10, events_dropped=40, batches_sent=30,
          buffer_usage=5%, success_rate=99.67%
```

| 메트릭 | 설명 |
|--------|------|
| `events_received` | 총 수신 이벤트 수 |
| `events_processed` | Kafka 전송 성공 수 |
| `events_failed` | Kafka 전송 실패 수 |
| `events_dropped` | 버퍼 초과로 거부된 수 |
| `batches_sent` | 전송된 배치 수 |
| `buffer_usage` | 버퍼 사용률 (%) |
| `success_rate` | 성공률 (%) |

---

## 7. Docker 구성

### 컨테이너 구조

```yaml
services:
  ingestor-1:     # 첫 번째 인스턴스 (포트: 8081)
  ingestor-2:     # 두 번째 인스턴스 (포트: 8082)
  ingestor-3:     # 세 번째 인스턴스 (포트: 8083)
  nginx-lb:       # 로드밸런서 (포트: 8080)
  kafka-healthcheck:  # Kafka 준비 확인
```

### 헬스체크 설정

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
  interval: 10s      # 10초마다 체크
  timeout: 5s        # 5초 내 응답 필요
  retries: 3         # 3회 실패 시 unhealthy
  start_period: 30s  # 시작 후 30초는 무시
```

---

## 8. 실행 방법

### 전체 클러스터 실행

```bash
# 1. Kafka 클러스터 시작
docker compose -f infra/kafka/docker-compose.yml up -d

# 2. Ingestor 클러스터 시작
cd ingestor
docker compose up -d --build

# 3. 로그 확인
docker compose logs -f
```

### 부하 테스트

```bash
# wrk 벤치마크 (12스레드, 400연결, 30초)
wrk -t12 -c400 -d30s -s post.lua http://localhost:8080/ingest
```

---

## 9. 설계 결정 및 트레이드오프

### Q: 왜 Spring WebFlux를 사용했나?

**A**:
- Netty 기반 비동기 논블로킹 I/O
- 스레드 풀 고갈 없이 높은 동시성 처리
- Reactor Kafka와 자연스러운 통합

### Q: 버퍼 크기가 10,000인 이유?

**A**:
- 너무 작으면: 트래픽 스파이크 시 429 다량 발생
- 너무 크면: 메모리 부족, 지연 시간 증가
- 10,000은 약 1-2초 분량의 버퍼 (초당 5,000-10,000 이벤트 가정)

### Q: acks=1을 사용한 이유?

**A**:
- `acks=all`: 모든 복제본 확인 → 지연 시간 증가
- `acks=1`: 리더만 확인 → 빠른 응답
- 이 시스템에서는 처리량이 우선이며, 극소수 유실은 허용 가능

### Q: least_conn vs round_robin?

**A**:
- Round-robin: 단순하지만 처리 시간이 다르면 불균형
- Least-conn: 실제 부하 기반 분산 → 더 균일한 분배
