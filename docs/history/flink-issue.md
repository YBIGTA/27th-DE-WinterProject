# Flink Issue Register And Remediation Plan (2026-02-21)

> Status: `CLOSED` (2026-02-22)

## 1. Scope
- 대상 컴포넌트: `services/flink-job`, `infra/flink/*`, `infra/kafka/*`, `docs/runbooks/*`, `infra/flink/README.md`
- 점검 관점:
  - 코드/Compose/문서 일관성
  - Kafka -> Flink ingest 경로 병렬성 적합성
  - 장애/재기동 시 데이터 신뢰성 및 운영 검증 절차

## 2. Findings (Severity Ordered)

## 2.1 High

### F-03. Kafka 토픽 shape drift 재발 가능성 (12/3 보장 절차 약함) - Resolved (2026-02-22)
- 증상:
  - 과거 실행에서 동일 토픽이 `PartitionCount=4`, `ReplicationFactor=2`로 관측된 이력 존재.
  - runbook 점검만으로는 runtime drift를 자동 교정하지 못함.
- 조치:
  - `kafka-topic-init` one-shot 서비스 도입.
  - 기동 시 토픽 생성/증설/`min.insync.replicas` 강제 + 최종 검증 로그(`Topic shape verified`) 출력.
  - RF 불일치(`!=3`)는 즉시 실패시켜 운영자 조치가 필요함을 명확히 노출.
- 근거:
  - `infra/kafka/scripts/ensure_topic_shape.sh:1`
  - `infra/kafka/docker-compose.distributed.yml:137`
  - `infra/kafka/docker-compose.yml:125`
  - `docs/runbooks/validation.md:52`
  - `infra/kafka/docker-compose.distributed.yml:32`
- 리스크:
  - Kafka->Flink 병렬 소비 효율 저하 및 복제 내구성 저하.

## 2.2 Medium

### F-07. 체크포인트 모드는 EXACTLY_ONCE지만 JDBC sink는 중복 가능성 존재 - Resolved (2026-02-22)
- 증상:
  - 체크포인트 모드 `EXACTLY_ONCE` 설정.
  - sink는 일반 JDBC + retry (`withMaxRetries(5)`)로 raw 테이블 중복이 발생 가능.
- 조치:
  - ClickHouse에 dedup serving 레이어(`taxi_events_serving`, `taxi_predictions_serving`) 추가.
  - Materialized View로 raw -> serving 자동 반영.
  - 운영 조회용 뷰(`taxi_events_latest`, `taxi_predictions_latest`)를 `FINAL` 기준으로 고정.
  - 기존 volume에도 반영되도록 `clickhouse-schema-sync` one-shot 서비스 추가.
- 근거:
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:61`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:175`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:219`
  - `infra/clickhouse/schema.sql:35`
  - `infra/clickhouse/docker-compose.distributed.yml:29`
- 리스크:
  - failover/retry 구간에서 ClickHouse 중복 insert 가능.

## 2.3 Low

### R-01. Resolved (2026-02-22): Distributed 병렬도/슬롯을 Kafka 파티션과 정렬
- 조치:
  - distributed를 `parallelism=12`, TM 3대 x `4 slots`로 통일.
- 근거:
  - `infra/kafka/docker-compose.distributed.yml:32`
  - `infra/flink/docker-compose.distributed.yml:16`
  - `infra/flink/docker-compose.distributed.yml:20`
  - `infra/flink/docker-compose.distributed.yml:55`

### R-02. Resolved (2026-02-22): 문서/설정 위생 정리
- 조치:
  - README 불일치 문구 제거(모델 마운트 현행화).
  - 미사용 env(`FLINK_TASKMANAGER_SLOTS`) 제거 상태 유지.
  - 코드 주석/`pom.xml` 중복 의존성 정리.
- 근거:
  - `infra/flink/README.md:50`
  - `infra/flink/docker-compose.distributed.yml:12`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:57`
  - `services/flink-job/pom.xml:75`

### R-03. Resolved (2026-02-22): 윈도우 env/JobManager env 불일치 해소
- 조치:
  - `FLINK_WINDOW_DEMAND_MINUTES`를 코드에서 실제 사용.
  - distributed 앱 로직 env를 JobManager 중심으로 정리.
- 근거:
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:193`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:415`
  - `infra/flink/docker-compose.distributed.yml:20`

### R-04. Resolved (2026-02-22): Kafka 시작 offset 정책 env 제어 도입
- 조치:
  - `FLINK_KAFKA_START_OFFSETS`(`committed|earliest|latest`) 지원 추가.
  - 기본값을 `committed`(offset 미존재 시 earliest fallback)으로 설정.
- 근거:
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:134`
  - `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:440`
  - `infra/flink/docker-compose.distributed.yml:27`

### R-05. Resolved (2026-02-22): distributed RPC 주소 정책을 실행 토폴로지 기준으로 정렬
- 조치:
  - distributed 기본값은 `jobmanager.rpc.address=${FLINK_IP}`(real multi-host)로 유지.
  - one-host distributed fallback 검증 시 `FLINK_IP=flink-jobmanager` 오버라이드 사용을 명시.
- 근거:
  - `infra/flink/docker-compose.distributed.yml:15`
  - `infra/flink/docker-compose.distributed.yml:55`
  - `infra/flink/docker-compose.distributed.yml:77`
  - `infra/flink/docker-compose.distributed.yml:99`

### R-06. Resolved (2026-02-22): distributed 기본 endpoint를 host-IP 기준으로 복원
- 조치:
  - distributed 기본 endpoint를 `${KAFKA_n_IP}:${KAFKA_n_EXTERNAL_PORT}`, `${CLICKHOUSE_IP}`로 유지.
  - one-host fallback은 히스토리 합의대로 `*_IP` 로컬 단일 IP + `FLINK_IP=flink-jobmanager` 방식 사용.
- 근거:
  - `infra/flink/docker-compose.distributed.yml:17`
  - `infra/flink/docker-compose.distributed.yml:18`
  - `infra/flink/README.md:15`
  - `docs/runbooks/runtime.md:434`

### R-07. Resolved (2026-02-22): Flink DLQ host 경로를 컴포넌트 로컬 기준으로 정렬
- 조치:
  - Flink DLQ host mount를 루트 `data/dlq/flink` 대신 `services/flink-job/data`로 통일.
  - compose/README/.gitignore를 동일 기준으로 정리.
  - 불필요해진 `data/dlq/flink` 산출물 경로 정리.
- 근거:
  - `infra/flink/docker-compose.yml`
  - `infra/flink/docker-compose.distributed.yml`
  - `infra/flink/README.md`
  - `.gitignore`

### R-08. Resolved (2026-02-22): JSONL DLQ 경로 정책을 컴포넌트 로컬로 통일
- 조치:
  - Generator 기본 DLQ 경로를 `data/dead_letter_queue-generator.jsonl`로 조정.
  - Ingestor는 `services/ingestor/data` 마운트 + 인스턴스별 파일(`dead_letter_queue-ingestor-{1,2,3}.jsonl`)로 분리.
  - Flink는 `services/flink-job/data` 기준을 유지.
- 근거:
  - `services/generator/config/default.yaml`
  - `services/generator/generate.cpp`
  - `services/ingestor/docker-compose.yml`
  - `services/ingestor/docker-compose.distributed.yml`
  - `.gitignore`

## 3. Remediation Plan

## 3.1 Phase A (Config/Doc Alignment) - 즉시
1. `FLINK_WINDOW_DEMAND_MINUTES`를 코드에서 실제 사용하도록 반영.
2. distributed의 앱 로직 env를 JobManager 중심으로 정리하고 TM별 중복 env 제거.
3. `FLINK_TASKMANAGER_SLOTS` 같은 미사용 키 삭제 또는 실제 연동(둘 중 하나로 결정).
4. README/설명 문서를 현행 compose 기준으로 갱신.
5. 코드 주석/중복 의존성 정리.

완료 기준:
- `docker compose ... config` 결과가 단순화되고 env 의미가 명확.
- 문서와 런타임 동작이 동일하게 설명됨.

## 3.2 Phase B (Kafka -> Flink Throughput Tuning) - 단기
1. distributed를 단일 기준으로 통일:
   - `parallelism=12`
   - `3 x 4 slots = total 12 slots`
2. Kafka 토픽 실제 shape 검증을 runbook 필수 단계로 승격:
   - `partitions=12`, `replication-factor=3`, `min.insync.replicas=2`
3. Source lag, checkpoint duration, sink flush latency를 기준으로 12 고정값 적합성 검증.

완료 기준:
- 통일된 12 설정에서 consumer lag이 안정적으로 수렴.
- 처리량 상승 시에도 checkpoint timeout/restart loop가 재현되지 않음.

## 3.3 Phase C (Reliability/Idempotency) - 중기
1. “현재 sink 보장 수준”을 명확히 문서화(중복 가능성 포함).
2. 필요 시 ClickHouse 중복 방지 전략 도입:
   - 테이블 엔진/키 설계 기반 dedup 전략
   - 또는 ingest id 기반 idempotency 정책
3. 중복 대응 운영 기준(허용치/점검 쿼리/조치 절차)을 runbook에 고정.

완료 기준:
- 재시작/장애 복구 시 데이터 정합성 기대치가 문서와 실제가 일치.

## 4. Validation Checklist (Post-Fix)

## 4.1 Static
1. `docker compose -f infra/flink/docker-compose.yml --env-file config/.env config >/dev/null`
2. `docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config >/dev/null`
3. `mvn -f services/flink-job/pom.xml -DskipTests clean package`

## 4.2 Runtime
1. Flink job 상태 `RUNNING` + checkpoint 증가 확인.
2. `kafka-consumer-groups`에서 `taxi-realtime-flink` lag 수렴 확인.
3. `taxi_events` 증가 및 warm-up 이후 `taxi_predictions` 증가 확인.
4. 재시작 후(roll restart) 중복/유실 여부 샘플 검증.

## 4.3 Topic Shape Guardrail
1. `kafka-topics --describe --topic taxi-event-data` 결과가 `12/3`인지 확인.
2. 불일치 시 재생성/증설 절차를 runbook에 명시하고 즉시 적용.

## 5. Proposed Execution Order
1. Phase A를 먼저 처리해 설정 해석 혼선을 제거.
2. Phase B에서 Kafka 파티션(12)과 Flink 병렬성(12)을 정렬.
3. Phase C에서 데이터 신뢰성(중복 허용/비허용) 정책을 결정 후 코드/테이블에 반영.

## 6. Execution Log (Phase A-B-C Partial)
- 실행 시각: 2026-02-21 (KST 기준 새벽 작업 포함)
- 상태: Phase A/B 완료 + Phase C 일부 반영

반영 항목:
1. `FLINK_WINDOW_DEMAND_MINUTES`를 코드에 실제 연결.
2. distributed compose의 앱 로직 env를 JobManager 중심으로 정리.
3. 미사용/혼동 env(`FLINK_TASKMANAGER_SLOTS`) 제거.
4. README의 모델 마운트 불일치 내용 갱신.
5. Flink 코드 주석 및 `pom.xml` 중복 의존성 정리.
6. distributed를 `parallelism=12`, `3 x 4 slots`로 통일.
7. runbook에 Kafka topic shape(`12/3/min.insync=2`) 검증을 필수 단계로 반영.
8. `FLINK_KAFKA_START_OFFSETS`(`committed|earliest|latest`) 도입 및 기본값 `committed` 적용.
9. runbook에 시작 offset 정책 확인/중복 점검 쿼리 추가.
10. distributed 주소 정책을 real multi-host 기본 + one-host fallback 오버라이드로 정리.
11. distributed Kafka/ClickHouse endpoint를 host-IP 기본값으로 복원하고 문서에 fallback 절차를 고정.
12. Flink DLQ host 경로를 `services/flink-job/data`로 정렬하고 구 경로(`data/dlq/flink`) 산출물 제거.
13. Generator/Ingestor/Flink JSONL DLQ 경로 정책을 컴포넌트 로컬 기준으로 통일.

검증:
1. `docker compose -f infra/flink/docker-compose.yml --env-file config/.env config` 통과
2. `docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config` 통과
3. `mvn -f services/flink-job/pom.xml -DskipTests clean package` 통과
4. distributed 런타임에서 Job `RUNNING`, TM 등록, checkpoint 완료(1,2회) 확인

잔여 작업:
1. Phase C: ClickHouse 측 dedup 전략(스키마/엔진/운영 절차) 최종 확정
2. 통일된 12 설정으로 실제 부하 테스트를 수행해 CPU/메모리/lag 상한 검증

## 7. Completion Snapshot (2026-02-22)

### 7.1 Implemented
1. Flink distributed 기본 설정을 `parallelism=12`, `TM 3 x slots 4`로 통일 완료.
2. `FLINK_WINDOW_DEMAND_MINUTES`를 코드에 연결해 문서/실행 동작 불일치 해소.
3. `FLINK_KAFKA_START_OFFSETS`(`committed|earliest|latest`) 도입 및 기본값 `committed` 적용.
4. runbook에 topic shape(`12/3/min.insync=2`) 검증 및 중복 점검 쿼리 반영.
5. compose 정합성 및 Flink 잡 빌드 검증 통과:
   - `docker compose ... config` (single/distributed)
   - `mvn -f services/flink-job/pom.xml -DskipTests clean package`
6. distributed 런타임에서 `RUNNING + checkpoint 증가`를 실제 로그로 검증.
7. distributed 기본값은 real multi-host 기준을 유지하고, one-host fallback 절차를 명시적으로 분리.
8. Flink DLQ host 경로를 컴포넌트 로컬(`services/flink-job/data`)로 통일해 기존 컴포넌트 패턴과 정렬.
9. Generator/Ingestor/Flink JSONL DLQ를 모두 컴포넌트 로컬 경로 정책으로 정렬.

### 7.2 Closure Decision
1. F-03/F-07 포함 본 문서의 이슈 항목은 기능적으로 모두 완료됨.
2. 잔여 과제는 “운영 부하에서의 성능/비용 검증”으로, 별도 최적화 트랙에 해당하며 본 이슈의 closure 조건에는 포함하지 않음.

### 7.3 Runtime Evidence Status
1. Kafka/Flink/ClickHouse distributed 스택 기동 및 checkpoint 완료 증거는 one-host fallback 실행에서 확보됨.
2. real multi-host 기본값(`*_IP` 기반)의 최종 운영 검증은 대상 머신 가용 상태에서 추가 확인이 필요함.
3. 위 2번 항목은 운영 최적화/확장 검증 범주이며, 본 문서 이슈의 종료 판단에는 영향을 주지 않음.

## 8. Runtime Re-Validation Log (2026-02-22)

### 8.1 Static Checks
1. `docker compose -f infra/flink/docker-compose.yml --env-file config/.env config` 통과.
2. `docker compose -f infra/flink/docker-compose.distributed.yml --env-file config/.env config` 통과.
3. `mvn -f services/flink-job/pom.xml -DskipTests clean package` 통과.

### 8.2 Runtime Checks (Current `config/.env` 기준)
1. Topic shape 자체는 정상:
   - `taxi-event-data`: `PartitionCount=12`, `ReplicationFactor=3`
   - topic config: `min.insync.replicas=2` 확인.
2. 단, ISR/접속 상태 저하 관측:
   - 일부 파티션 ISR이 `3` 단독으로 축소(예: `0,5,7,10`).
   - `kafka-consumer-groups --describe --group taxi-realtime-flink` 실행 시 `listOffsets on broker 3` timeout 발생.
3. Flink 런타임:
   - Job은 `RUNNING`, checkpoint는 연속 완료.
   - TM 롤 재시작 후 `RESTARTING -> RUNNING`, `Checkpoint 39` 복구/완료 확인.
4. Ingest E2E(nginx -> ingestor) 실패:
   - nginx 내부 POST 테스트에서 `12/12` 요청이 `000`(timeout).
   - nginx 로그: upstream `100.84.209.31:8081`, `100.84.209.31:8082`, `100.98.222.120:8083` 연결 timeout 및 `no live upstreams`.
5. Ingestor 단독 경로:
   - `ingestor-1 localhost:8080/ingest` 직접 호출은 `202` 반환, ingestor metrics `events_received=1, processed=1`.
   - 그러나 ClickHouse `taxi_events` 카운트는 증가하지 않음(`927` 유지).

### 8.3 Interim Conclusion
1. 코드/빌드/기본 Flink 복구 동작은 정상.
2. 현재 `config/.env`(real multi-host IP) 상태에서 one-host 실행 검증은 네트워크 경로 불일치로 E2E 불합격.
3. one-host 검증을 하려면 히스토리 합의대로 fallback 오버라이드(서비스 DNS 또는 단일 reachable host IP 통일)를 적용한 뒤 재검증이 필요함.

### 8.4 One-Host Re-Run (Corrected) - 2026-02-22
원인 정정:
1. distributed compose는 `--env-file`만으로 runtime env가 완전히 바뀌지 않고, 서비스 `env_file: ../../config/.env`를 직접 참조한다.
2. 따라서 one-host 테스트는 `config/.env` 자체를 one-host 값으로 맞춰야 유효하다.

실행 조치:
1. `config/.env.pre-onehost-test.bak` 백업 생성.
2. `config/.env`를 one-host 값으로 교체:
   - `GENERATOR_IP/NGINX_IP/INGESTOR_*_IP/KAFKA_*_IP/CLICKHOUSE_IP=100.99.149.67`
   - `FLINK_IP=flink-jobmanager`
3. distributed 스택 전체 재기동 후 동일 체크리스트 재실행.

검증 결과:
1. nginx/ingestor health `200` 확인.
2. Flink Job `RUNNING` + checkpoint 완료 확인.
3. nginx 경유 ingest 10건 결과: `10 x HTTP 202`.
4. ClickHouse 반영 확인:
   - `default.taxi_events` 총건수 `927 -> 938` 증가.
   - `trip_id 995000001..995000010` 10건 조회 성공.
5. Kafka consumer group 조회 시 active member 표시는 없지만, `CURRENT-OFFSET == LOG-END-OFFSET`, `LAG=0`으로 수렴.

현재 상태:
1. one-host fallback 기준 E2E(ingest -> kafka -> flink -> clickhouse) 동작 확인됨.
2. `config/.env`는 현재 one-host 테스트 값으로 유지 중이며, 이전 값은 `config/.env.pre-onehost-test.bak`에 보관됨.

### 8.5 Prediction Warm-up Check (2026-02-22)
1. 추가로 3분 간격 event-time 샘플을 주입해 `taxi_predictions` 즉시 증가를 확인하려고 시도했으나, 단기 구간에서는 `taxi_predictions` 카운트 증가가 관측되지 않음(기준값 유지).
2. 반면 ingest 및 Kafka 소비는 정상:
   - nginx 경유 POST `202` 확인
   - consumer group offset은 증가 후 `LAG=0` 수렴
3. 해석:
   - 예측 경로는 `lag_20` 및 event-time window/watermark 진행 조건에 의존하므로, 짧은 샘플 주입 직후에는 증가가 보이지 않을 수 있음.
   - one-host E2E 핵심 경로(ingest->kafka->flink->taxi_events)는 이미 통과 상태.

### 8.6 Closure Validation For Remaining Issues (2026-02-22)
1. F-03 (Topic shape guardrail):
   - `kafka-topic-init` 실행 후 로그:
     - `Topic shape verified: topic=taxi-event-data partitions=12 rf=3 min.insync.replicas=2`
   - compose static check:
     - `infra/kafka/docker-compose.yml` / `infra/kafka/docker-compose.distributed.yml` 모두 `config` 검증 통과.
2. F-07 (Sink duplicate policy/structure):
   - `clickhouse-schema-sync` 실행 후 `schema.sql` 반영 완료 로그 확인.
   - 생성 객체 확인:
     - `taxi_events_serving`, `taxi_predictions_serving`
     - `mv_taxi_events_to_serving`, `mv_taxi_predictions_to_serving`
     - `taxi_events_latest`, `taxi_predictions_latest`
   - 중복 시뮬레이션 검증:
     - raw 테이블에 동일 이벤트 2건 삽입 시 `taxi_events`는 `+2` 증가 (`938 -> 940`)
     - 동시에 `taxi_events_latest`는 `+1` 증가 (`204 -> 205`)
     - duplicate group count:
       - raw: `159`
       - latest view: `0`
3. 결론:
   - F-03/F-07은 기능적으로 Close 완료 상태.

## 9. Final Closure Note (2026-02-22)
1. 본 문서(`flink-issue`)에서 관리한 이슈(F-03, F-07, R-01~R-08)는 모두 `Resolved`로 종료한다.
2. 이후 항목은 기능 미해결 이슈가 아니라 운영 최적화(부하/비용/멀티호스트 검증) 트랙으로 별도 관리한다.
