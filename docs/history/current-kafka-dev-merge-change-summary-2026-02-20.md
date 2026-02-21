# Kafka-Dev Merge Change Summary (2026-02-20)

## 1. 기준 커밋
- 머지 커밋: `d2febd6229047fb1de64b2d3119fe7813f6f0930`
- 메시지: `Merge branch 'kafka-dev' into end2end`
- 작성/커밋 시각: 2026-02-20 11:28:57 +0900
- 부모 커밋:
  - 1st parent: `287bedd` (merge 전 `end2end` 상태)
  - 2nd parent: `44029d8` (merge 당시 `kafka-dev` tip)

## 2. 변경 해석 관점

### 관점 A: `end2end`에 kafka-dev가 유입되며 추가된 변경
- 비교식: `d2febd6^1..d2febd6`
- 결과: 17 files changed, 196 insertions(+), 116 deletions(-)
- 해석: `end2end` 입장에서 머지로 새로 들어온 "실행/인프라 중심" 변경

### 관점 B: kafka-dev 기준에서 머지 결과까지의 차이
- 비교식: `d2febd6^2..d2febd6`
- 결과: 50 files changed, 3409 insertions(+), 679 deletions(-)
- 해석: `kafka-dev`에 없던 `end2end` 측 문서/운영 구조까지 포함된 전체 차이

## 3. 관점 A 상세 (핵심 런타임 변경)

## 3.1 Kafka 설정 강화
- 파일:
  - `infra/kafka/docker-compose.yml`
  - `infra/kafka/docker-compose.distributed.yml`
- 공통 변경:
  - `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2 -> 3`
  - `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 2 -> 3`
  - `KAFKA_DEFAULT_REPLICATION_FACTOR: 2 -> 3`
  - `KAFKA_NUM_PARTITIONS: 4 -> 12`
  - `KAFKA_MIN_INSYNC_REPLICAS: 2` 추가

## 3.2 Nginx LB 라우팅/연결 정책 변경
- 파일:
  - `infra/nginx/nginx.single-machine.conf`
  - `infra/nginx/templates/nginx.distributed.conf.template`
- 변경:
  - `least_conn` -> `random two least_conn`
  - upstream server에서 `max_fails=3 fail_timeout=30s` 제거
  - `keepalive 32 -> keepalive 64`

## 3.3 Ingestor 처리량 관련 튜닝 변경
- 파일:
  - `services/ingestor/docker-compose.distributed.yml`
  - `services/ingestor/docker-compose.yml`
  - `services/ingestor/src/main/java/com/ingestion/config/IngestorTuningProperties.java`
  - `services/ingestor/src/main/java/com/ingestion/config/ReactorKafkaConfig.java`
  - `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java`
  - `services/ingestor/bin/main/application.yml` (신규)
- 변경:
  - distributed env:
    - buffer `10000 -> 100000`
    - batch size `500 -> 1000`
    - timeout `50ms -> 10ms`
    - send concurrency `4 -> 12`
  - single-machine env:
    - send concurrency `8 -> 12`
  - Java 기본 튜닝값:
    - buffer size `10000 -> 30000`
    - batch timeout `50 -> 10`
  - producer 옵션:
    - `linger.ms 50 -> 10`
    - `batch.size 32768 -> 65536`
    - `buffer.memory 64MB -> 256MB`
    - `max.block.ms 2000 -> 5000`
  - `FAIL_NON_SERIALIZED` 처리:
    - 기존 `emitNext + failure handler` 중심
    - 변경 후 `tryEmitNext` 재시도 루프(최대 10회, `Thread.onSpinWait()`)

## 3.4 Flink 배포/잡 설정 변경
- 파일:
  - `infra/flink/docker-compose.yml`
  - `infra/flink/docker-compose.distributed.yml`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java`
- single-machine compose 변경:
  - `parallelism.default: 6 -> 12`
  - restart strategy(fixed-delay, attempts=100, delay=10s) 추가
  - Kafka bootstrap: 내부 DNS 기반 -> `${KAFKA_n_IP}:${KAFKA_n_EXTERNAL_PORT}` 기반
  - ClickHouse host: `clickhouse -> ${CLICKHOUSE_IP}`
  - `FLINK_PARALLELISM: 6 -> 12`
  - JDBC batch `20000 -> 50000`, interval `1000 -> 3000`
  - taskmanager slot `2 -> 4` (각 TM)
- distributed compose 변경:
  - JobManager의 `jobmanager.rpc.address: ${FLINK_IP} -> flink-jobmanager`
  - `parallelism.default: 6 -> 3`
  - 일부 TM slot/env 값 재배치 및 변경
  - 파일 하단 `networks` 블록이 제거된 상태로 머지됨
- Flink job 코드 변경:
  - checkpointing(EXACTLY_ONCE) 및 checkpoint 옵션 추가
  - watermark idleness(`withIdleness`) 제거
  - late-event 출력 코드 주석 처리
  - ClickHouse sink parallelism `3` 명시
  - 기본 parallelism `6 -> 12`
  - JDBC batch `5000 -> 50000`, interval `1000 -> 3000`

## 3.5 ClickHouse 적재/파티션 정책 변경
- 파일:
  - `infra/clickhouse/config.xml` (신규)
  - `infra/clickhouse/users.xml` (신규)
  - `infra/clickhouse/docker-compose.yml`
  - `infra/clickhouse/schema.sql`
- 변경:
  - MergeTree 보호 설정 추가:
    - `parts_to_delay_insert=2000`
    - `parts_to_throw_insert=4000`
  - async insert profile 추가:
    - `async_insert=1`
    - `wait_for_async_insert=0`
    - `async_insert_busy_timeout_ms=2000`
    - `async_insert_max_data_size=104857600`
  - 테이블 파티션:
    - `toYYYYMM(ts) -> toYYYYMMDD(ts)` (월 단위 -> 일 단위)

## 4. 관점 B 요약 (kafka-dev 대비 머지 결과)
- 비교식: `d2febd6^2..d2febd6`
- 큰 변경군:
  - 문서 체계 확장:
    - `docs/current/current-logging-implementation.md` 추가
    - `docs/history/*`, `docs/logging/*`, `docs/runbooks/*` 추가/이관
    - `docs/templates/explanation.template.md` 추가
    - `docs/system-architecture.md` 추가
  - 설명 문서:
    - `infra/kafka/EXPLANATION.md`, `infra/nginx/EXPLANATION.md` 추가
    - `services/generator/EXPLANATION.md`, `services/ingestor/EXPLANATION.md` 수정
    - `config/EXPLANATION.md` 수정
  - 임시 데이터흐름 문서:
    - `temp/generator-data-flow-ko.md`
    - `temp/ingestor-data-flow-ko.md`
    - `temp/kafka-data-flow-ko.md`
    - `temp/nginx-data-flow-ko.md`

## 5. 머지 직후 확인 포인트
- Flink distributed compose:
  - `kafka-network` 참조와 top-level networks 정의 일치 여부
  - JobManager/TaskManager `jobmanager.rpc.address` 일관성
- Flink single-machine compose:
  - 컨테이너 내부에서 external IP 기반 bootstrap/host를 사용하는 의도 재확인
- Ingestor:
  - retry/emit 정책 변경 후 backlog 상황에서의 안정성 검증
- 문서:
  - `temp/*` 및 `EXPLANATION.md`가 실제 최신 설정값(RF=3, partitions=12, nginx 정책)과 일치하는지 재검증

## 6. 참고 명령
```bash
git show --no-patch --pretty=fuller d2febd6
git diff --name-status d2febd6^1..d2febd6
git diff --stat d2febd6^1..d2febd6
git diff --name-status d2febd6^2..d2febd6
git diff --shortstat d2febd6^2..d2febd6
```
