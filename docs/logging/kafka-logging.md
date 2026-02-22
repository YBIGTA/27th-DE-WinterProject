# Kafka Logging Guide

## 1) 목적
이 문서는 Kafka를 "흐름 설명"이 아니라 "현재 상태 품질 + 병목 위치 식별 + 성능 검증" 관점으로 운영 점검하기 위한 로깅/모니터링 기준이다.

핵심 목표:
- 브로커 간 리더/복제 파티션 편차 감지
- ISR 축소 및 under-replicated 상태 조기 감지
- 토픽/파티션별 적재 진행 상태 추적
- 컨슈머 lag 상태와 회복 속도 추적
- 병목 지점(요청 큐/핸들러/디스크/네트워크) 빠른 분리
- 성능 기준(처리량/지연/회복시간) 충족 여부 확인
- 장애를 추적하는 데 필요한 상태 스냅샷 표준화

---

## 2) 기본 원칙
- `INFO`: 10초~60초 주기 요약 상태
- `WARN`: 편차 증가, ISR 축소, lag 급증, 요청 지연 증가
- `ERROR`: URP 지속, min ISR 위반, lag 회복 실패, controller/metadata 이상
- `DEBUG`: 특정 파티션/브로커 상세 추적 시 샘플링으로 한시 활성화

권장:
- "메시지 payload" 로깅 금지
- "메트릭 요약 + 임계치 경보 + 상태 스냅샷" 조합 사용
- 경보 로그에는 최소 `type`, `scope`, `duration`, `suspect`, `action` 필드를 포함

---

## 3) 로깅 항목

### 3.1 Broker 균형 상태
목적:
- 브로커별 leader 쏠림과 replica 부담 편차를 확인

권장 지표:
- `leader_partitions_by_broker`
- `replica_partitions_by_broker`
- `leader_skew_pct` (`(max_leader - min_leader) / avg_leader * 100`)
- `topic_partition_count`
- `hot_partition_ratio_by_topic` (토픽 내 상위 파티션 쏠림)

경보:
- `leader_skew_pct`가 임계치 초과 상태로 지속
- 특정 broker의 leader 수가 장시간 급감 또는 0
- hot partition 비율 급증

### 3.2 복제 건강 상태
목적:
- 데이터 내구성 저하 신호를 빠르게 감지

권장 지표:
- `under_replicated_partitions`
- `out_of_sync_replicas_count`
- `at_min_isr_partition_count`
- `isr_shrink_events` / `isr_expand_events`
- `produce_not_enough_replicas_error_rate`
- `unclean_leader_election_count`

경보:
- URP가 지속적으로 0으로 회복되지 않음
- ISR shrink 이벤트 급증
- `NOT_ENOUGH_REPLICAS` 계열 produce 실패 발생

### 3.3 파티션 진행 상태
목적:
- 파티션별 적재/소비가 정상적으로 증가하는지 확인

권장 지표:
- `partition_current_offset` 증가율
- `partition_log_end_offset` 분포
- `bytes_in_rate_by_topic`
- `messages_in_rate_by_topic`
- `bytes_out_rate_by_topic`
- `produce_to_consume_gap_rate` (in-rate 대비 out-rate 갭)

경보:
- 특정 파티션 offset 증가가 장시간 정체
- 입력 트래픽 대비 특정 파티션만 과도하게 급증
- in-rate 유지 중 out-rate 급감

### 3.4 Consumer 상태
목적:
- 데이터가 "쌓이기만" 하지 않고 소비되고 있는지 확인

권장 지표:
- `consumer_group_lag_total`
- `consumer_group_lag_by_topic_partition`
- `max_lag_partition`
- `max_lag_age_sec`
- `lag_recovery_rate`
- `stalled_consumer_groups`

경보:
- lag 총량이 지속 증가
- 특정 그룹/파티션 lag가 장시간 정체
- lag age가 임계치 이상으로 지속

### 3.5 Control plane 상태
목적:
- KRaft 메타데이터/리더 선출 관련 이상 징후 감지

권장 지표:
- broker up/down 상태
- controller quorum 관련 에러 로그 패턴
- controller election 빈도
- metadata propagation 지연 징후

경보:
- broker down 상태가 quorum 여유를 잠식
- controller election 반복
- controller 관련 에러 패턴 반복

### 3.6 Broker 요청 큐/지연 (병목 핵심)
목적:
- lag 증가의 원인이 consumer가 아니라 broker 처리 포화인지 식별

권장 지표:
- `request_queue_size`
- `request_handler_avg_idle_pct`
- `network_processor_avg_idle_pct`
- `produce_request_queue_time_ms_p95`
- `fetch_request_queue_time_ms_p95`
- `produce_request_total_time_ms_p95`
- `fetch_request_total_time_ms_p95`
- `request_timeout_error_rate`

경보:
- request queue 증가 + handler idle 하락이 동반
- produce/fetch 요청 p95 지연이 임계치 이상으로 지속
- timeout 에러율 급증

### 3.7 Broker 리소스 압박
목적:
- 저장소/리소스 포화로 인한 처리율 저하와 장애 위험을 조기 탐지

권장 지표:
- `broker_disk_used_pct`
- `broker_disk_io_util_pct`
- `broker_disk_write_latency_ms_p95`
- `broker_cpu_used_pct`
- `broker_network_rx_bytes_rate` / `broker_network_tx_bytes_rate`
- `time_to_disk_full_estimate_hour`

경보:
- 디스크 사용률 임계 접근
- 디스크 I/O util 고수준 지속 + request latency 동반 악화
- 네트워크 또는 CPU 포화 장기 지속

### 3.8 End-to-End 성능 검증
목적:
- "정상 동작"이 아니라 "목표 성능 충족" 여부를 정량 판정

권장 지표:
- `producer_ack_latency_ms_p95` (ingestor/generator 측)
- `produce_error_rate`
- `end_to_end_event_latency_ms_p95` (생성 시각 -> 소비/처리 시각)
- `throughput_baseline_ratio` (현재 처리량 / 기준 처리량)
- `lag_recovery_half_life_sec`

경보:
- ack latency p95가 기준 초과로 지속
- throughput이 기준선 대비 장시간 저하
- lag 회복 반감기가 목표보다 악화

---

## 4) 지표 소스와 해석

### 4.1 소스 계층
현재 스택:
- Kafka exporter: `danielqsj/kafka-exporter` (lag, partition, replica 상태)
- Kafka JMX exporter: `bitnami/jmx-exporter` + broker JMX (request queue, handler/network idle, request latency)
- Prometheus scrape: 기본 `15s`

권장 확장:
- Node exporter/cAdvisor: 디스크/CPU/네트워크 계열
- Producer/Consumer 앱 메트릭: ack latency, end-to-end latency

### 4.2 해석 원칙
- exporter 버전에 따라 label/metric 이름이 달라도 "의미 축"을 고정한다.
- 알림 룰은 metric name이 아니라 의미 축(`URP`, `lag_recovery`, `request_queue_pressure`) 기준으로 작성한다.
- 운영 문서에는 "실제 metric name 매핑표"를 별도 유지한다.

예시 metric 축:
- `kafka_topic_partition_leader`
- `kafka_topic_partition_replicas`
- `kafka_topic_partition_in_sync_replica`
- `kafka_topic_partition_under_replicated_partition`
- `kafka_topic_partition_current_offset`
- `kafka_consumergroup_lag`
- `kafka_request_queue_size` (JMX 매핑)
- `kafka_request_handler_idle_pct` (JMX 매핑)
- `node_disk_io_util_pct` (node exporter 매핑)

---

## 5) 상태 스냅샷 표준

주기 요약 예시:
```text
[KFK_METRICS] ts=... win=15s brokers_up=3 leader_skew_pct=6.2 urp=0 isr_shrink=0 topic_partitions=12 msg_in_rate=... bytes_in_rate=... lag_total=... lag_recovery_rate=... req_q=... req_idle=... produce_p95_ms=... disk_used_pct=...
```

경보 예시:
```text
[KFK_WARN] type=broker_queue_pressure broker=kafka-2 req_q=1240 req_idle=12.3% produce_p95_ms=188 duration=6m suspect=broker_cpu_or_disk action=check_jmx_and_node_metrics
```

```text
[KFK_WARN] type=leader_skew_spike broker=kafka-2 leader_partitions=78 peer_avg=42 hot_partition_ratio=34.2% action=check_partition_reassignment_and_key_distribution
```

```text
[KFK_ERROR] type=urp_persistent urp=9 at_min_isr=5 duration=8m action=check_broker_health_isr_network_disk_and_consider_producer_throttle
```

---

## 6) 운영 체크리스트
- broker별 leader partition 편차가 임계치 내에 있는가?
- URP가 0으로 회복되는가?
- ISR shrink가 반복되는가?
- topic/partition offset이 입력량과 일관되게 증가하는가?
- consumer lag가 회복 가능한 기울기를 가지는가?
- request queue/handler idle로 broker 병목 여부가 분리되는가?
- produce/fetch p95 지연이 허용 범위 내인가?
- 디스크 사용률/IO util이 임계 접근 중인가?
- broker down 시 quorum 여유가 유지되는가?

---

## 7) 성능 관점 메모
- 이벤트 단위 상세 로그 상시 출력은 금지하고 주기 집계(`10s~60s`)를 기본으로 사용한다.
- 파티션 단위 메트릭은 고카디널리티 위험이 있으므로 상위 N개(`topk`) 중심으로 노출한다.
- 운영 기본은 `INFO` 집계 + `WARN/ERROR` 임계치 경보이며, 상세 원인 추적 시에만 `DEBUG`를 샘플링으로 켠다.
- Kafka 병목은 대부분 "요청 큐/디스크 I/O/핫 파티션" 조합으로 드러나므로 3축을 항상 함께 본다.

---

## 8) 임계치 초안 (운영 시작값)
- `leader_skew_pct >= 25%` 10분 지속 -> `WARN`
- `leader_skew_pct >= 40%` 10분 지속 -> `ERROR`
- `under_replicated_partitions > 0` 3분 지속 -> `WARN`
- `under_replicated_partitions > 0` 10분 지속 -> `ERROR`
- `at_min_isr_partition_count > 0` 5분 지속 -> `WARN`
- `produce_not_enough_replicas_error_rate > 0` 1분 지속 -> `ERROR`
- `consumer_group_lag_total` 증가 + `lag_recovery_rate <= 0` 10분 지속 -> `WARN`
- `max_lag_age_sec >= 300` 10분 지속 -> `WARN`
- `request_handler_avg_idle_pct < 20%` 5분 지속 -> `WARN`
- `request_handler_avg_idle_pct < 10%` 5분 지속 + `request_queue_size` 증가 -> `ERROR`
- `produce_request_total_time_ms_p95 >= 100ms` 5분 지속 -> `WARN`
- `produce_request_total_time_ms_p95 >= 250ms` 5분 지속 -> `ERROR`
- `broker_disk_used_pct >= 80%` -> `WARN`, `>= 90%` -> `ERROR`
- `broker_disk_io_util_pct >= 85%` 10분 지속 -> `WARN`
- `controller_election_count_delta_10m >= 3` -> `ERROR`

---

## 9) 병목 진단 런북 (빠른 분기)

### 9.1 lag 총량이 증가할 때
1. `bytes_in_rate` vs `bytes_out_rate`를 먼저 비교한다.
2. `request_queue_size` 상승 + `request_handler_avg_idle_pct` 하락이면 broker 병목으로 분류한다.
3. broker가 정상인데 특정 group/partition만 lag가 크면 consumer 병목으로 분류한다.
4. `hot_partition_ratio`가 높으면 키 분포/파티션 설계를 우선 점검한다.

### 9.2 URP/ISR 이슈가 지속될 때
1. broker up/down, 네트워크 손실, 디스크 지연을 동시 점검한다.
2. `at_min_isr_partition_count`와 produce 실패율을 함께 본다.
3. 회복이 지연되면 producer rate 제한 또는 배치 축소로 압력을 낮춘다.

### 9.3 produce latency 급증 시
1. `produce_request_queue_time_ms_p95`와 `produce_request_total_time_ms_p95`를 함께 확인한다.
2. `broker_disk_io_util_pct`/`broker_disk_write_latency_ms_p95`가 동반 상승하면 저장소 병목 우선 대응.
3. queue는 낮은데 latency만 오르면 네트워크 또는 downstream ack 경로를 점검한다.

### 9.4 controller election 반복 시
1. quorum 구성 노드의 헬스/네트워크 안정성을 먼저 확인한다.
2. broker 재시작 루프/자원 포화를 확인한다.
3. election 빈도 정상화 전까지 불필요한 운영 변경(재할당/대규모 배포)을 보류한다.

---

## 10) 성능 검증 시나리오 (배포 전/정기 점검)
1. 정상 부하 30분:
   - 목표 처리량에서 `lag_total` 안정, `produce_p95` 기준 이내 유지
2. 버스트 부하(기준의 2~3배, 10분):
   - queue 상승 후 회복, `lag_recovery_half_life_sec` 목표 이내
3. consumer 일시 중지/재개:
   - lag 급증 후 목표 시간 내 회복 여부 검증
4. broker 1대 장애 유도:
   - URP 발생 후 회복 시간, controller 안정성, min ISR 위반 여부 확인

성공 판정:
1. URP가 지속되지 않고 지정 시간 내 0으로 복귀
2. 병목 발생 시 원인이 runbook 분기로 5분 내 분리됨
3. throughput/latency/lag 회복 지표가 기준선 이내

---

## 11) 추가 권장
- `kafka-topics --describe` 스냅샷을 정기 저장해 leader/replica 분포 변경 이력을 남긴다.
- 주요 토픽에 대해 파티션별 lag 상위 N개를 별도 패널로 분리한다.
- 브로커 리밸런싱 수행 시 전/후 leader skew 비교 로그를 남긴다.
- alert rule과 Grafana 패널 이름을 동일 키(`leader_skew`, `urp`, `request_queue_pressure`)로 맞춰 운영 혼선을 줄인다.

---

## 12) Prometheus/Loki 정석 매핑 (2026-02-22)

### 12.1 Prometheus 수집 경로 (active, dual-run)
1. `kafka-exporter` (`danielqsj/kafka-exporter`)
   - 역할: lag/offset/partition/replica 상태
   - target:
     - single/distributed 공통 `kafka-exporter:9308`
2. `kafka-jmx` (`bitnami/jmx-exporter` + broker JMX)
   - 역할: request queue, handler/network idle, request latency, broker 내부 상태
   - target:
     - single: `jmx-exporter-kafka-{1,2,3}:5556`
     - distributed:
       - `${KAFKA_1_IP}:${KAFKA_1_JMX_EXPORTER_PORT:-9404}`
       - `${KAFKA_2_IP}:${KAFKA_2_JMX_EXPORTER_PORT:-9405}`
       - `${KAFKA_3_IP}:${KAFKA_3_JMX_EXPORTER_PORT:-9406}`
3. 설정 파일:
   - `infra/kafka/docker-compose.yml`
   - `infra/kafka/docker-compose.distributed.yml`
   - `infra/kafka/jmx-exporter/kafka-kraft-3_0_0-rules.yml`
   - `infra/prometheus/prometheus.yml.tmpl`
   - `infra/prometheus/prometheus.distributed.yml`

### 12.2 역할 분담 고정
- `kafka-exporter`를 기본 소스로 쓰는 축:
  - `consumer_group_lag_total`
  - partition offset/leader/replica 상태
  - URP/ISR 상태
- `kafka-jmx`를 기본 소스로 쓰는 축:
  - `request_queue_size`
  - `request_handler_avg_idle_pct`
  - `network_processor_avg_idle_pct`
  - produce/fetch request queue/total latency

운영 원칙:
- 같은 의미 축에 소스가 2개일 때는 1차 소스를 고정하고, 나머지는 교차검증용으로 유지한다.
- dual-run 종료 전까지 대시보드에 `source=kafka_exporter|kafka_jmx`를 명시한다.

### 12.2.1 dual-run 의미축 매핑표 (실 metric 기준)
아래 표를 alert/recording rule의 1차 매핑 기준으로 고정한다.

| 의미 축 | 1차 source | 1차 metric/query | 2차 교차검증 |
|---|---|---|---|
| consumer lag total | kafka-exporter | `sum(kafka_consumergroup_lag)` | `kafka_server_brokertopicmetrics_messagesin_total` 증가율과 동행성 확인 |
| max lag partition | kafka-exporter | `topk(1, kafka_consumergroup_lag)` | 동일 파티션의 produce/fetch 지연(JMX) 확인 |
| URP(under-replicated) | kafka-exporter | `sum(kafka_topic_partition_under_replicated_partition)` | `kafka_server_replicamanager_isrshrinks_total` 증가율 |
| ISR shrink/expand | kafka-jmx | `rate(kafka_server_replicamanager_isrshrinks_total[5m])` / `rate(kafka_server_replicamanager_isrexpands_total[5m])` | exporter의 URP/ISR 상태 |
| leader skew | kafka-exporter | `stddev(kafka_topic_partition_leader)`, broker별 leader count 집계 | controller/metadata 관련 로그 |
| broker request queue pressure | kafka-jmx | `max(kafka_network_requestchannel_requestqueuesize)` | lag 증가와 동시 발생 여부 |
| request handler idle | kafka-jmx | `avg(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent)` | queue size 상승과 동반 여부 |
| network processor idle | kafka-jmx | `avg(kafka_network_socketserver_networkprocessoravgidle_percent)` | produce/fetch total time p95 동반 상승 여부 |
| produce/fetch latency p95 | kafka-jmx | `max(kafka_network_requestmetrics_totaltimems{request=\"Produce\",quantile=\"0.95\"})`, `max(kafka_network_requestmetrics_totaltimems{request=~\"Fetch.*\",quantile=\"0.95\"})` | ingestor ack latency, flink source lag |
| bytes/messages in rate | kafka-jmx | `rate(kafka_server_brokertopicmetrics_bytesin_total[1m])`, `rate(kafka_server_brokertopicmetrics_messagesin_total[1m])` | exporter offset 증가율 |

메모:
- JMX metric 이름은 `infra/kafka/jmx-exporter/kafka-kraft-3_0_0-rules.yml` 기준이다.
- Kafka 버전/도메인 차이로 metric name이 바뀌면 "의미 축"은 고정하고 매핑표만 업데이트한다.

### 12.3 Loki 보완 범위
- Prometheus 경보(URP, lag, queue pressure) 이후 원인 분석
- 필수 이벤트:
  - broker/controller error
  - quorum/election 반복
  - network timeout, disk IO error
- 필수 필드:
  - `broker`, `partition`, `controller`, `type`, `action`

### 12.4 시작 쿼리 (예시)
- PromQL:
  - lag total: `sum(kafka_consumergroup_lag)`
  - URP: `sum(kafka_topic_partition_under_replicated_partition)`
  - broker queue pressure: `max({__name__=~"kafka_.*request_queue_size.*"})`
- LogQL:
  - controller 오류: `{service=~"kafka.*"} |= "controller" |= "ERROR"`
  - ISR shrink 이벤트: `{service=~"kafka.*"} |= "ISR" |= "shrink"`

### 12.5 dual-run 종료 판단 기준 (후속)
아래 3가지를 만족하면 `kafka-exporter`/`kafka-jmx` 병행 수집을 축소 검토한다.

1. 2주 이상 운영에서 경보 탐지 성능이 동등하거나 우위임이 확인될 것
2. 의미 축별(lag/URP/queue/latency) 누락 없이 대체 가능할 것
3. 대시보드/rule/온콜 런북 수정 비용이 운영 이득보다 작을 것
