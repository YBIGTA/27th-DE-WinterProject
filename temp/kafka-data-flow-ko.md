# Kafka 데이터 흐름 설명 (한국어)

기준 흐름:

[Ingestor Producers] --(Kafka Produce)--> [Kafka Cluster: 3 Brokers (KRaft)] --(Kafka Consume)--> [Flink / Kafka Connect / Other Consumers]

## 1. 입력: Kafka로 들어오는 트래픽
- 주요 입력은 Ingestor가 보내는 `taxi-event-data` 토픽 레코드입니다.
- 단일 머신 기준으로 브로커 외부 포트는 broker별 `9092 / 9094 / 9096`을 사용합니다.
- 분산 모드에서는 각 브로커의 실제 IP/PORT(`KAFKA_1_IP`, `KAFKA_1_EXTERNAL_PORT` 등)를 `config/.env`에서 읽어 advertise합니다.
- compose 실행 시 `--env-file config/.env`를 반드시 사용해야 listener/advertised 주소가 의도대로 치환됩니다.

## 2. 클러스터 구성: 3노드 KRaft
- `kafka-1`, `kafka-2`, `kafka-3` 모두 `KAFKA_PROCESS_ROLES=broker,controller`로 동작합니다.
- 노드 식별자는 고정(`KAFKA_NODE_ID=1/2/3`)이며, 동일한 `CLUSTER_ID`를 공유합니다.
- 컨트롤러 합의는 `KAFKA_CONTROLLER_QUORUM_VOTERS`로 3개 노드 엔드포인트를 지정합니다.
- ZooKeeper 없이 KRaft 메타데이터 쿼럼으로 브로커/컨트롤러 기능을 함께 수행합니다.

## 3. 리스너/주소 노출 방식
- 브로커는 3개 listener role을 가집니다.
  - `EXTERNAL`: 외부 클라이언트 접속
  - `INTERNAL`: 브로커 간/내부 네트워크 접속
  - `CONTROLLER`: KRaft 컨트롤러 통신
- Single-machine:
  - `KAFKA_LISTENERS`의 `INTERNAL/CONTROLLER` 포트는 공통 env 기본값(`29092`, `19093`)을 공유합니다.
  - `KAFKA_ADVERTISED_LISTENERS`의 INTERNAL은 `kafka-1`, `kafka-2`, `kafka-3` DNS명을 사용합니다.
- Distributed:
  - broker별 internal/controller host 포트를 분리(`29092/29094/29096`, `19093/19094/19095`)해 충돌을 방지합니다.
  - `KAFKA_ADVERTISED_LISTENERS`가 broker별 실제 IP를 노출합니다.

## 4. 레코드 쓰기/읽기 경로
- Producer가 bootstrap 서버 중 하나로 접속한 뒤 메타데이터를 받아 대상 partition leader로 전송합니다.
- 기본 설정상 토픽 자동 생성이 켜져 있어(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`), 미존재 토픽 첫 produce 시 생성될 수 있습니다.
- 자동 생성 토픽의 기본 파티션 수는 `KAFKA_NUM_PARTITIONS=4`입니다.
- 기본 복제 팩터는 `KAFKA_DEFAULT_REPLICATION_FACTOR=2`로 설정되어 3브로커 중 2복제 기준으로 동작합니다.
- Consumer(Flink/Connect)는 동일 토픽을 독립 consumer group으로 읽어 fan-out 처리합니다.

## 5. 상태 저장: 로그/세그먼트/보존 정책
- 각 브로커 데이터는 `/var/lib/kafka/data`에 저장됩니다.
- compose에서 broker별 볼륨(`kafka1-data`, `kafka2-data`, `kafka3-data`)을 분리해 재시작 시 데이터가 유지됩니다.
- 로그 보존 정책:
  - `KAFKA_LOG_RETENTION_HOURS=24`
  - `KAFKA_LOG_SEGMENT_BYTES=1073741824` (1GB)
- 트랜잭션/오프셋 내부 토픽 안정성을 위해 아래 값이 고정됩니다.
  - `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=2`
  - `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=2`
  - `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2`

## 6. 상태 확인과 운영 관찰
- 각 브로커 healthcheck는 컨테이너 내부 `localhost:29092`에 `kafka-broker-api-versions`를 수행합니다.
- `kafka-ui`는 별도 컨테이너로 구동되어 토픽/파티션/컨슈머 상태를 조회합니다.
- Single-machine UI bootstrap은 internal DNS(`kafka-1:29092,...`)를 사용하고, distributed UI는 외부 advertised 주소(`KAFKA_n_IP:KAFKA_n_EXTERNAL_PORT`)를 사용합니다.

## 7. 전체 흐름을 한 줄로 요약
1. Ingestor가 Kafka bootstrap 주소로 레코드를 보낸다.  
2. Kafka 클러스터가 KRaft 쿼럼을 통해 메타데이터와 리더 상태를 유지한다.  
3. 레코드는 partition leader에 append되고 broker 볼륨에 영속 저장된다.  
4. Flink/Connect가 각자 consumer group으로 같은 토픽을 병렬 소비한다.  
5. Kafka UI/healthcheck로 클러스터 상태를 운영 관점에서 확인한다.

## 8. 단계별 실행 타임라인 (순서 중심)
1. `.env` 로드  
   compose가 `--env-file config/.env`로 IP/PORT 변수를 읽습니다.
2. broker 컨테이너 기동  
   `kafka-1/2/3`가 cp-kafka 이미지로 시작합니다.
3. listener 바인딩  
   EXTERNAL/INTERNAL/CONTROLLER 소켓을 열고 advertised 주소를 확정합니다.
4. KRaft quorum 형성  
   controller voter 목록 기반으로 메타데이터 쿼럼을 구성합니다.
5. healthcheck 통과  
   `kafka-broker-api-versions --bootstrap-server localhost:29092` 성공 시 healthy가 됩니다.
6. producer 접속  
   Ingestor producer가 bootstrap broker에 메타데이터 요청을 보냅니다.
7. 리더 라우팅  
   대상 topic/partition 리더 브로커로 produce request가 전달됩니다.
8. 로그 append/복제  
   리더 append 후 복제 설정에 따라 팔로워 복제가 진행됩니다.
9. consumer poll  
   Flink/Connect consumer가 토픽 offset을 기준으로 데이터를 읽습니다.
10. 오프셋/내부 상태 갱신  
    consumer group offset, transaction state 등 내부 토픽 상태가 갱신됩니다.
11. 운영 관찰  
    Kafka UI 또는 exporter/Prometheus/Grafana 경로로 메트릭을 확인합니다.
12. 재시작/복구  
    컨테이너 재기동 시 broker 볼륨의 기존 로그를 기반으로 상태를 복구합니다.

## 9. 데이터 객체 이동 맵 (어디서 -> 어디로)
| 단계 | 어디에 있음 | 데이터 형태 | 다음 이동 |
|---|---|---|---|
| 1 | Ingestor producer | `ProducerRecord(topic,key,value)` | bootstrap broker |
| 2 | bootstrap broker | metadata 응답(leader/partition 정보) | producer |
| 3 | leader broker | produce request batch | partition log append |
| 4 | follower broker(s) | replicated log entries | ISR/replica state 갱신 |
| 5 | broker local storage | segment files + index | 보존/롤링 관리 |
| 6 | consumer (Flink/Connect) | polled records | stream processing / sink |
| 7 | Kafka internal topics | offsets/txn/group 상태 | 재시작 후 상태 복원 |
| 8 | Kafka UI | broker/topic/consumer 메타데이터 | 운영자 조회 화면 |

## 10. Kafka가 실제로 관리하는 것
1. 브로커 합의 상태  
   KRaft controller quorum으로 클러스터 메타데이터와 리더십을 관리합니다.
2. 토픽/파티션 로그 영속성  
   각 broker의 로컬 디스크 볼륨에 append-only 로그를 저장합니다.
3. 복제와 내고장성  
   replication factor, ISR 조건으로 데이터 가용성과 일관성을 균형 조정합니다.
4. 소비 진행 상태  
   consumer group offset과 transaction 상태를 내부 토픽으로 관리합니다.
5. 네트워크 진입점 관리  
   EXTERNAL/INTERNAL/CONTROLLER listener로 클라이언트/브로커/컨트롤러 트래픽을 분리합니다.

## 11. 전체 흐름 (Mermaid)
```mermaid
flowchart TD
    B1[kafka-1 broker+controller (distributed)]
    B2[kafka-2 broker+controller (distributed)]
    B3[kafka-3 broker+controller (distributed)]
    Q[KRaft controller quorum]

    P[Ingestor Producer]
    TP[(topic taxi-event-data\npartitions)]
    V1[(kafka1-data)]
    V2[(kafka2-data)]
    V3[(kafka3-data)]

    F[Flink Consumer Group]
    KC[Kafka Connect Group]
    UI[Kafka UI]

    B1 --> Q
    B2 --> Q
    B3 --> Q

    P -->|produce| TP
    TP -->|stored on leaders/replicas| V1
    TP -->|stored on leaders/replicas| V2
    TP -->|stored on leaders/replicas| V3

    TP -->|consume| F
    TP -->|consume| KC

    UI -->|metadata/topic/group view| B1
    UI -->|metadata/topic/group view| B2
    UI -->|metadata/topic/group view| B3
```

해석 포인트:
- Kafka는 애플리케이션 레벨 변환 로직이 아니라, 로그 저장/복제/전달(transport + durability) 레이어를 담당합니다.
- Single-machine과 distributed의 차이는 “리스너 advertise 대상”과 “포트 매핑 전략”이며, 클러스터 논리(3-node KRaft)는 동일합니다.
- `--env-file config/.env` 누락 시 advertised 주소가 잘못 치환되어 producer/consumer 연결 실패로 이어질 수 있습니다.
