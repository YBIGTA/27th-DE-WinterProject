---
component: Kafka Connect S3 Sink
status: CURRENT
last_reviewed: 2026-02-21
core_files:
  - infra/connectors/s3-sink-config.template.json
  - infra/connectors/README.md
---

# Kafka Connect S3 Sink

## Role
이 컴포넌트는 Kafka의 특정 토픽(`taxi-event-data`)에 들어온 데이터를 실시간으로 감지하여, AWS S3 데이터 레이크에 JSON 파일 형태로 적재하는 역할을 수행합니다. 데이터 파이프라인에서 브로커와 스토리지 사이를 잇는 다리 역할을 합니다.

## I/O Flow
```
[Kafka Topic: taxi-event-data] --(Kafka Protocol)--> [This Component] --(AWS S3 API)--> [S3 Bucket: nyc-taxi-raw-...]
```
- **Input:** `taxi-event-data` Kafka 토픽으로 들어오는 스키마가 없는(Schema-less) JSON 메시지
- **Output:** S3 버킷(`nyc-taxi-raw-2026-ybigta-de`)에 날짜 기반으로 파티셔닝된 JSON 파일

## Implementation Logic

이 컴포넌트는 코드가 아닌, Kafka Connect 프레임워크 위에서 동작하는 Confluent S3 Sink Connector의 **설정(Configuration)**입니다. `s3-sink-config.json` 파일을 Kafka Connect REST API를 통해 전달하면, 설정에 명시된 대로 동작하는 커넥터가 생성됩니다.

### Data Flow
```mermaid
flowchart TD
    subgraph Kafka
        A[Topic: taxi-event-data]
    end

    subgraph "Kafka Connect Worker"
        B(S3 Sink Connector Tasks)
        B -- " Consume" --> A
        C{Buffer <br> (flush.size=3)}
        B -- "Accumulate" --> C
    end
    
    subgraph "AWS S3"
        D[S3 Bucket]
    end

    C -- "3개 메시지 도달 시 Flush" --> D
    
    subgraph "S3 Object Key"
       E["/year=YYYY/month=MM/day=dd/..."]
    end

    D -- "TimeBasedPartitioning" --> E

```

### Concurrency Model
- **Task Model:** `tasks.max`가 `4`로 설정되어 있어, Kafka Connect가 토픽 파티션을 최대 4개 태스크에 분산해 병렬 처리할 수 있습니다. 실제 활성 태스크 수는 워커 수와 파티션 수에 따라 달라집니다.
- **State Management:** 소비한 토픽의 오프셋(Offset)과 같은 커넥터의 상태는 Kafka Connect가 내부 토픽을 사용하여 관리합니다. 따라서 커넥터가 재시작되어도 마지막으로 처리했던 지점부터 작업을 이어갈 수 있습니다.

### Core Algorithm
1.  **토픽 구독:** `topics` 설정에 명시된 `taxi-event-data` 토픽을 구독합니다.
2.  **JSON 변환:** `JsonConverter`를 사용하여 Kafka 메시지의 Value를 스키마 정보 없이 순수 JSON으로 해석합니다 (`value.converter.schemas.enable: false`).
3.  **데이터 축적:** `flush.size`에 설정된 값(`3`)만큼 메시지가 메모리 버퍼에 쌓일 때까지 기다립니다.
4.  **S3 업로드:** 버퍼가 차면, 축적된 메시지들을 하나의 S3 객체로 묶어 `ap-northeast-2` 리전의 `nyc-taxi-raw-2026-ybigta-de` 버킷에 업로드합니다.
5.  **타임스탬프 변환 (SMT):** 메시지 본문에 포함된 `ts` 필드(ISO 8601 형식의 문자열)를 `TimestampConverter` SMT(Single Message Transform)를 사용하여 Unix 타임스탬프(long)로 변환합니다.
6.  **파티셔닝:** `TimeBasedPartitioner`를 사용하여 **5번 단계에서 변환된 `ts` 필드의 값**을 기준으로 `year=YYYY/month=MM/day=dd` 형식의 디렉터리 구조를 만들어 파일을 저장합니다 (`timestamp.extractor: RecordField`).

## Data Contract
- **Input:**
  - **Topic:** `taxi-event-data`
  - **Key:** String (`StringConverter`)
  - **Value:** Schema-less JSON (`JsonConverter` with `schemas.enable=false`)
- **Output:**
  - **Format:** JSON (`JsonFormat`)
  - **Location:** S3 Bucket (`nyc-taxi-raw-2026-ybigta-de`)
  - **Path:** `.../year=YYYY/month=MM/day=dd/<topic>+<partition>+<start-offset>.json`
- **Invariants:**
  - Kafka 토픽의 메시지는 유효한 JSON 형식이어야 합니다.
  - **Kafka 토픽 메시지는 `ts` 키를 가져야 하며, 값은 `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` 형식의 ISO 8601 문자열이어야 합니다.**
  - 커넥터에 설정된 AWS 자격증명은 대상 S3 버킷에 대한 쓰기 권한을 가지고 있어야 합니다.

## Design Decisions
| Decision                        | Why                                                                                                                                 | Trade-off                                                                                                                                                             |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Schema-less JSON 처리**       | 데이터 소스(Generator)가 스키마 레지스트리를 사용하지 않고 순수 JSON을 생성하므로, 이에 맞춰 `schemas.enable=false`로 설정했습니다. | 스키마가 없으므로 데이터의 무결성을 커넥터 단계에서 검증할 수 없습니다. 데이터 구조가 변경될 경우 후처리 단계에서 오류가 발생할 수 있습니다.                          |
| **이벤트 시간 기준 파티셔닝**   | S3에 저장되는 데이터의 날짜 기준을 데이터 처리 시점이 아닌, **이벤트가 실제 발생한 시점(`ts` 필드)**으로 사용하기 위함입니다. 이를 통해 데이터 파이프라인에 지연이 발생하더라도 데이터의 시간적 정합성을 보장할 수 있습니다. | `TimestampConverter`를 사용하여 `ts` 필드를 변환하므로, 토픽으로 들어오는 모든 메시지는 반드시 `ts` 필드를 포함해야 하며, 그 값은 `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` 형식과 일치해야 합니다. 형식이 맞지 않으면 커넥터 작업(Task)이 실패합니다. |
| **작은 `flush.size` (3)**       | 개발 및 테스트 환경에서 데이터가 S3에 적재되는 것을 빠르게 확인하기 위함입니다.                                                     | 실제 운영 환경에서는 `flush.size`가 너무 작으면 S3 API 호출이 빈번해지고, 작은 파일이 많이 생성되어 성능 저하의 원인이 됩니다. (운영 시 1000 이상 권장)               |
| **`Key Converter=String`**      | 메시지 Key는 단순 식별자(String)로 들어오는 경우가 많아 JSON 파싱 에러를 방지하기 위함입니다.                                       | Key에 복잡한 구조체(Struct) 정보를 담을 수 없음                                                                                                                       |

## Failure Modes & Handling
| Failure                                                              | Detection                                                            | Response                                                                                                                                                                                                   |
| -------------------------------------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DataException: JsonConverter with schemas.enable requires "schema"` | Kafka Connect 로그에 해당 예외가 출력됩니다.                         | 커넥터가 스키마를 요구하는데 토픽의 메시지에 스키마가 없는 상황입니다. `s3-sink-config.json` 설정에 `"value.converter.schemas.enable": "false"`가 포함되어 있는지 확인하고 커넥터를 재배포합니다.          |
| S3에 파일이 생성되지 않음                                            | Kafka에 데이터가 있음에도 불구하고 S3 버킷에 파일이 보이지 않습니다. | `flush.size`(현재 3)만큼 데이터가 쌓이지 않아 커넥터가 버퍼링하며 대기 중일 가능성이 높습니다. Kafka Producer를 통해 더 많은 메시지를 보내거나, 즉시 확인이 필요하면 `flush.size`를 1로 낮춰 재배포합니다. |
| `AmazonS3Exception: Access Denied`                                   | Kafka Connect 로그에 S3 접근 거부 예외가 출력됩니다.                 | 커넥터에 설정된 `aws.access.key.id`와 `aws.secret.access.key`가 대상 S3 버킷에 대한 쓰기 권한이 없는 경우입니다. Terraform을 통해 생성된 IAM 사용자의 키가 올바르게 입력되었는지 확인합니다.               |
