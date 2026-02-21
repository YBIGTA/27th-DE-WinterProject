# Ingestor 데이터 흐름 설명 (한국어)

기준 흐름:

[Generator] --(HTTP POST /ingest, /ingest/batch)--> [Nginx LB] --(random two least_conn)--> [Ingestor] --(Reactive Sink + Batch Send)--> [Kafka: taxi-event-data]

## 1. 입력: HTTP 요청 수신
- Ingestor는 WebFlux(reactive) 기반으로 HTTP 요청을 받습니다.
- 주요 엔드포인트:
  - `POST /ingest`: 단건 `TaxiEvent`
  - `POST /ingest/batch`: `TaxiEvent` 배열
  - `GET /health`: 문자열 `"OK"` 반환
- `POST /ingest/batch`에서 배열이 비어 있거나 `null`이면 `400 Bad Request`를 반환합니다.

## 2. Controller 단계 처리
- `IngestionController`는 각 요청을 `IngestionService.ingest()`로 넘겨 Sink 적재 결과를 기준으로 응답 코드를 정합니다.
- 단건(`/ingest`) 응답:
  - `202 Accepted`: 적재 성공(`Sinks.EmitResult.OK`)
  - `429 Too Many Requests`: Sink overflow
  - `503 Service Unavailable`: `FAIL_NON_SERIALIZED` (예외 케이스)
  - `500 Internal Server Error`: 기타 emit 실패
- 배치(`/ingest/batch`) 응답:
  - 이벤트를 순차 처리하며 각 결과를 카운트
  - 중간에 overflow 발생 시 즉시 중단 후 `429` 반환
  - 전체 성공 `202`, 부분 성공 `207 Multi-Status`, 전체 실패 `500`
  - 바디는 `BatchIngestResponse { acceptedCount, rejectedCount, failedIndices }`

## 3. Sink 적재와 백프레셔
- 내부 버퍼는 `Sinks.many().multicast().onBackpressureBuffer(bufferSize, false)`로 생성됩니다.
- `ingest()` 동작:
  - 먼저 `tryEmitNext(event)` 시도
  - `FAIL_NON_SERIALIZED`면 `tryEmitNext`를 bounded retry(최대 10회, 50us -> 2ms 백오프)로 재시도
  - `FAIL_OVERFLOW`면 드롭하고 `eventsDropped` 증가
- 버퍼 크기는 `app.tuning.buffer.size` (코드 기본 30,000)이며, 환경변수 `APP_TUNING_BUFFER_SIZE`로 오버라이드 가능합니다. (compose 프로파일에서는 100,000 사용)

## 4. 내부 비동기 파이프라인 구성
- `@PostConstruct`에서 Sink 소비 파이프라인을 구독합니다.
- 핵심 체인:
  - `sink.asFlux()`
  - `.bufferTimeout(batchSize, timeoutMs)`
  - `.flatMap(this::sendBatchToKafkaReactive, sendConcurrency)`
  - `.retry()` (파이프라인 단위 재구독)
- 튜닝 기본값:
  - `batchSize=500`
  - `timeoutMs=10`
  - `sendConcurrency=4`
  - `metricsIntervalSec=10`

## 5. 배치 -> Kafka 전송 경로
- `sendBatchToKafkaReactive(batch)`에서 각 이벤트를 `SenderRecord<String, String, Integer>`로 변환합니다.
- Kafka key는 `String.valueOf(event.getTripId())`, value는 `ObjectMapper` JSON 문자열입니다.
- 직렬화 실패 시 해당 이벤트를 파일 기반 DLQ(`DeadLetterQueue`)에 JSONL 형식으로 기록하고, 실패 카운터를 증가시킨 뒤, Kafka 전송에서는 건너뜁니다(`Mono.empty()`). DLQ 파일 경로는 `app.dlq.filepath`로 설정 가능합니다 (기본값: `dead_letter_queue.jsonl`).
- `kafkaSender.send(records)` 결과 처리:
  - `SenderResult.exception() != null`: 해당 레코드 실패로 카운트
  - 정상: `eventsProcessed` 증가
- `Retry.backoff(3, 100ms).maxBackoff(2s)`는 `send()` 스트림 에러 시 배치 재시도에 적용됩니다.

### 5-1. 어느 브로커로 보내는지는 누가 결정하는가?
- Ingestor 코드가 직접 `broker-1`, `broker-2`를 골라 보내는 방식은 아닙니다.
- Ingestor가 하는 일:
  - 토픽 지정 (`topicName`)
  - key 지정 (`tripId` -> String)
  - `kafkaSender.send()` 호출
- 이후 라우팅은 Kafka Producer 클라이언트(ingestor 프로세스 내부 라이브러리)가 처리합니다.
  1. bootstrap 서버 중 한 곳에 접속해 메타데이터를 조회
  2. key 기반으로 partition 결정(커스텀 partitioner를 따로 구현하지 않음)
  3. 해당 partition의 leader broker로만 전송
- 즉, "모든 브로커에 동시에 전송"이 아니라 "대상 partition leader로 전송"이 정확한 동작입니다.

## 6. Reactor Kafka 설정 특성
- `ReactorKafkaConfig`에서 Producer 옵션을 코드로 직접 설정합니다.
- 주요 값:
  - `acks=1` (leader ack만 기다림)
  - `compression.type=lz4`
  - `retries=3`
  - `max.in.flight.requests.per.connection=5`
  - `SenderOptions.stopOnError(false)`
  - `SenderOptions.maxInFlight(1024)`
- Sender는 lazy connection이라 앱 시작 시 Kafka 연결이 강제되지 않고, 첫 send 시점에 연결됩니다.

## 7. 전체 흐름을 한 줄로 요약
1. HTTP 이벤트를 받는다.  
2. Sink 버퍼에 적재한다.  
3. 버퍼 이벤트를 배치로 묶는다.  
4. Reactor Kafka로 비동기 전송한다.  
5. 실패는 emit 결과/레코드 예외/스트림 예외 레벨로 분리 처리하고, 직렬화 실패 이벤트는 DLQ 파일에 보존한다.

## 8. 단계별 실행 타임라인 (순서 중심)
1. 요청 수신  
   Generator가 Nginx를 거쳐 Ingestor의 `/ingest` 또는 `/ingest/batch`를 호출합니다.
2. DTO 역직렬화  
   WebFlux/Jackson이 JSON을 `TaxiEvent`(또는 리스트)로 변환합니다.
3. Sink 적재 시도  
   `tryEmitNext`로 즉시 적재를 시도합니다.
4. 동시 emit 충돌 처리  
   `FAIL_NON_SERIALIZED`면 `tryEmitNext`를 bounded retry(최대 10회)로 재시도합니다.
5. overflow 처리  
   버퍼 가득 참(`FAIL_OVERFLOW`)이면 드롭하고 `429` 경로로 응답합니다.
6. 배치 형성  
   Sink Flux에서 `bufferTimeout` 기준(개수/시간)으로 배치를 만듭니다.
7. Kafka 레코드 변환  
   각 이벤트를 `ProducerRecord(topic, key=tripId, value=json)`로 변환합니다.
8. Kafka 비동기 전송  
   `kafkaSender.send()` 호출 후 producer 내부에서
   `bootstrap 메타데이터 조회 -> key 기반 partition 결정 -> leader broker 전송` 순서로 처리합니다.
9. 전송 결과 반영  
   레코드 단위 성공/실패 카운터를 갱신합니다.
10. 스트림 오류 재시도  
    `Retry.backoff`로 배치 전송을 재시도하고, 파이프라인 종료 시 `.retry()`로 재구독합니다.
11. 주기 메트릭 로깅
    `eventsReceived/Processed/Failed/Dropped/Batches/DLQ written`을 주기적으로 로그에 남깁니다.
12. 종료 처리
    `@PreDestroy`에서 Sink complete -> 5초 대기 -> KafkaSender close -> DeadLetterQueue close 순으로 정리합니다.

## 9. 데이터 객체 이동 맵 (어디서 -> 어디로)
| 단계 | 어디에 있음 | 데이터 형태 | 다음 이동 |
|---|---|---|---|
| 1 | Nginx -> Ingestor HTTP layer | JSON body | WebFlux controller |
| 2 | Controller | `TaxiEvent` / `List<TaxiEvent>` | `IngestionService.ingest()` |
| 3 | Service ingress path | `Sinks.EmitResult` | 응답 코드 분기 |
| 4 | Sink buffer | `TaxiEvent` stream | `asFlux().bufferTimeout(...)` |
| 5 | Batch builder | `List<TaxiEvent>` | `sendBatchToKafkaReactive()` |
| 6 | Kafka sender input | `SenderRecord<String,String,Integer>` | `kafkaSender.send()` |
| 7 | Producer client (Ingestor 내부) | metadata + key 기반 partition 계산 | partition leader broker |
| 8 | Kafka broker | topic record(key=`tripId`) | partition append |
| 9 | DLQ 파일 | JSONL (직렬화 실패 이벤트) | 사후 분석/재처리 |
| 10 | Metrics counters | `AtomicLong` 누적값 | 주기 로그 출력 |

## 10. Ingestor가 실제로 관리하는 것
1. 수신-전송 분리  
   HTTP 수신 속도와 Kafka 전송 속도를 Sink 버퍼로 분리합니다.
2. 백프레셔 신호  
   overflow를 `429`로 노출해 상위(Generator) 재시도 로직이 동작하도록 합니다.
3. 배치 전송 효율  
   `bufferTimeout` + 동시 전송으로 처리량을 확보합니다.
4. 장애 지속성
   레코드 단위 실패 누적, 스트림 오류 재시도, 파이프라인 재구독으로 전송 루프를 유지합니다.
5. 실패 이벤트 보존
   직렬화 실패 이벤트를 파일 기반 DLQ(JSONL)에 기록하여 사후 분석과 재처리가 가능합니다.

## 11. 전체 흐름 (Mermaid)
```mermaid
flowchart TD
    G[Generator]
    N[Nginx LB random two least_conn]
    C[IngestionController]
    I[IngestionService.ingest]
    E{tryEmitNext result}
    S[(Sinks.Many buffer)]
    B[bufferTimeout batch]
    K[sendBatchToKafkaReactive]
    R[SenderRecord key=tripId value=json]
    KS[KafkaSender.send]
    META[Bootstrap broker metadata lookup]
    PART[Partition select by key(tripId)]
    LEAD[Send to target partition leader]
    KR{SenderResult.exception?}
    KP[(Kafka topic taxi-event-data)]
    RETRY[Retry.backoff + pipeline retry]
    M[(Atomic metrics counters)]
    DLQ[(DLQ file dead_letter_queue.jsonl)]

    G -->|POST /ingest or /ingest/batch| N --> C --> I --> E
    E -->|OK| S
    E -->|FAIL_NON_SERIALIZED| I
    E -->|FAIL_OVERFLOW| C

    S --> B --> K --> R --> KS --> META --> PART --> LEAD
    LEAD -->|acks=1 leader ack result| KR
    KR -->|No| KP
    KR -->|Yes| M
    KS -->|stream error| RETRY --> KS
    K -->|serialization failure| DLQ

    C -->|202/207/429/500| G
    I --> M
    K --> M
```

해석 포인트:
- `/ingest`의 `202`는 Kafka 저장 완료가 아니라 “내부 Sink 적재 성공” 의미입니다.
- `SenderResult.exception()`은 레코드 실패 처리이며, `Retry.backoff`는 스트림 오류 시 배치 레벨로 동작합니다.
- `tripId`가 `null`이면 key는 `"null"` 문자열이 됩니다(ingestor에서 non-null 검증하지 않음).
- Ingestor가 "브로커를 수동 선택"하지는 않으며, producer 라이브러리가 `metadata -> partition -> leader` 라우팅을 수행합니다.
- 현재 `acks=1`이라 leader ack만 성공 조건으로 보며, follower ack를 기다리는 설정은 아닙니다.
