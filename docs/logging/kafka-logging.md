# Kafka Logging Guide

## 1) 목적
이 문서는 Kafka를 "흐름 설명"이 아니라 "현재 상태 품질" 관점으로 운영 점검하기 위한 로깅/모니터링 기준이다.

핵심 목표:
- 브로커 간 리더/복제 파티션 편차 감지
- ISR 축소 및 under-replicated 상태 조기 감지
- 토픽/파티션별 적재 진행 상태 추적
- 컨슈머 lag 상태와 회복 속도 추적
- 장애를 추적하는 데 필요한 상태 스냅샷 표준화

---

## 2) 기본 원칙
- `INFO`: 주기 요약 상태 (Prometheus scrape 주기 15s 권장)
- `WARN`: 편차 증가, ISR 축소, lag 급증
- `ERROR`: URP 지속, lag 회복 실패, controller/metadata 이상
- `DEBUG`: 특정 파티션/브로커 상세 추적 시 한시 활성화

권장:
- "메시지 payload" 로깅 금지
- "메트릭 요약 + 임계치 경보 + 상태 스냅샷" 조합 사용

---

## 3) 로깅 항목

### 3.1 Broker 균형 상태
목적:
- 브로커별 리더 파티션 쏠림과 replica 부담 편차를 확인

권장 지표:
- `leader_partitions_by_broker`
- `replica_partitions_by_broker`
- `leader_skew_pct` (최대-최소/평균)
- `topic_partition_count` (증감 추적)

경보:
- `leader_skew_pct`가 임계치 초과 상태로 지속
- 특정 broker의 leader 수가 장시간 급감 또는 0

### 3.2 복제 건강 상태
목적:
- 데이터 내구성 저하 신호를 빠르게 감지

권장 지표:
- `under_replicated_partitions`
- `out_of_sync_replicas_count`
- `min_isr_violation_signals` (간접: produce 실패율/알람)
- `isr_shrink_events` / `isr_expand_events` 추이

경보:
- URP가 N분 이상 0으로 회복되지 않음
- ISR shrink 이벤트가 급증

### 3.3 파티션 진행 상태
목적:
- 파티션별 적재가 정상적으로 증가하는지 확인

권장 지표:
- `partition_current_offset` 증가율
- `partition_log_end_offset` 분포
- `bytes_in_rate_by_topic`
- `messages_in_rate_by_topic`

경보:
- 특정 파티션 offset 증가가 장시간 정체
- 입력 트래픽 대비 특정 파티션만 과도하게 급증

### 3.4 Consumer 상태
목적:
- 데이터가 "쌓이기만" 하지 않고 소비되고 있는지 확인

권장 지표:
- `consumer_group_lag_total`
- `consumer_group_lag_by_topic_partition`
- `lag_recovery_rate`
- `stalled_consumer_groups`

경보:
- lag 총량이 지속 증가
- 특정 그룹/파티션 lag가 장시간 정체

### 3.5 Control plane 상태
목적:
- KRaft 메타데이터/리더 선출 관련 이상 징후 감지

권장 지표:
- broker up/down 상태
- controller quorum 관련 에러 로그 패턴
- controller election 빈도

경보:
- broker down 상태가 quorum 여유를 잠식
- controller/election 관련 에러 패턴 반복

---

## 4) 지표 소스와 해석

현재 스택:
- Kafka exporter: `danielqsj/kafka-exporter`
- Prometheus scrape: 기본 `15s`

메트릭 명 참고:
- exporter 버전에 따라 label/metric 이름이 일부 다를 수 있다.
- 실운영에선 "의미 축(leader skew, URP, lag)"을 고정하고, 실제 이름은 환경에서 매핑한다.

예시 metric 축:
- `kafka_topic_partition_leader`
- `kafka_topic_partition_replicas`
- `kafka_topic_partition_in_sync_replica`
- `kafka_topic_partition_under_replicated_partition`
- `kafka_topic_partition_current_offset`
- `kafka_consumergroup_lag`

---

## 5) 상태 스냅샷 표준

주기 요약 예시:
```text
[KFK_METRICS] ts=... win=15s brokers_up=3 leader_skew_pct=6.2 urp=0 isr_shrink=0 topic_partitions=12 msg_in_rate=... bytes_in_rate=... lag_total=... lag_recovery_rate=...
```

경보 예시:
```text
[KFK_WARN] type=leader_skew_spike broker=kafka-2 leader_partitions=78 peer_avg=42 action=check_rebalance_and_broker_load
```

```text
[KFK_ERROR] type=urp_persistent urp=9 duration=8m action=check_broker_health_isr_network_disk
```

---

## 6) 운영 체크리스트
- broker별 leader partition 편차가 임계치 내에 있는가?
- URP가 0으로 회복되는가?
- ISR shrink가 반복되는가?
- topic/partition offset이 입력량과 일관되게 증가하는가?
- consumer lag가 회복 가능한 기울기를 가지는가?
- broker down 시 quorum 여유가 유지되는가?

---

## 7) 추가 권장
- `kafka-topics --describe` 스냅샷을 정기 저장해 leader/replica 분포 변경 이력을 남긴다.
- 주요 토픽에 대해 파티션별 lag 상위 N개를 별도 패널로 분리한다.
- 브로커 리밸런싱 수행 시 전/후 leader skew 비교 로그를 남긴다.
