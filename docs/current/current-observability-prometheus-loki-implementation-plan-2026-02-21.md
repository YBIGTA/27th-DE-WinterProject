# Observability Implementation Plan (Prometheus + Loki)

## 1) 목적
`docs/logging/*.md`에 정의한 운영 로깅 기준을 실제 런타임에서 수집/조회/알림 가능한 상태로 구현한다.

핵심 방향:
- 정량 지표는 `Prometheus`
- 이벤트/원인 추적은 `Loki`
- 대시보드/운영 확인은 `Grafana`

### 1.1 컴포넌트 착수 원칙 (명시)
각 컴포넌트 작업은 아래 순서를 **필수**로 따른다.

1. 시작 전 해당 `docs/logging/<component>-logging.md`를 먼저 읽고, 목표 metric/log/threshold를 작업 기준으로 확정한다.
2. 확정한 기준과 현재 코드/compose/promtail/prometheus 설정의 갭을 먼저 식별한 뒤 구현한다.
3. 구현 후에는 검증 쿼리/대시보드 확인까지 수행하고, 필요 시 `docs/logging`과 `docs/current`를 동기화한다.
4. metric으로 표현 가능한 항목은 모두 `Prometheus`에 우선 구현한다.
5. metric으로 표현이 어렵거나 원인 추적이 필요한 케이스(예외, 샘플 payload context, 상태 전이)는 `Loki`로 보완한다.
6. 경보 판단의 1차 기준은 Prometheus rule이며, Loki는 경보 원인 분석(triage) 용도로 사용한다.
7. 동일 책임의 exporter 후보가 2개 이상일 때는 초기에는 병행 수집(dual-run)으로 붙인 뒤, 대시보드/알림/운영 비용을 비교해 최종 1안으로 축소한다.

### 1.2 문서 역할 분리 규칙 (운영 합의)
앞으로 문서는 아래 역할로 분리해서 운영한다.

1. `docs/current/*`
   - 실행 순서, 명령, 오케스트레이션, 단계 진행 상태의 기준 문서
   - "지금 무엇을 먼저 할지"와 "무엇이 끝났는지"를 관리
2. `docs/logging/*`
   - 컴포넌트별 관측 사양(metric/log/threshold/query)의 기준 문서
   - 실제 구현명(metric/log key), 경보 임계, 런북을 고정

실행 루프:
1. `docs/current`에서 이번 사이클 작업 순서/명령을 확정한다.
2. 코드/인프라 변경을 적용한다.
3. 변경된 내용은 해당 `docs/logging/<component>-logging.md`에 먼저 반영한다.
4. 마지막에 `docs/current`에 변경 요약과 상태(`done/pending`)를 업데이트한다.

충돌 시 우선순위:
1. 관측 의미/지표 정의 충돌: `docs/logging` 우선
2. 작업 순서/범위/진행 상태 충돌: `docs/current` 우선

### 1.3 현재 적용 현황 (2026-02-22)
현재 턴에서 반영한 사항을 기록한다.

완료:
1. ClickHouse 공식 Prometheus endpoint 활성화
   - `infra/clickhouse/config.xml`
   - `infra/clickhouse/docker-compose.yml`
   - `infra/clickhouse/docker-compose.distributed.yml`
2. Flink 공식 PrometheusReporter 활성화 + JobManager/TaskManager metrics 포트 노출
   - `infra/flink/docker-compose.yml`
   - `infra/flink/docker-compose.distributed.yml`
3. Prometheus scrape 경로 보강
   - ClickHouse `/metrics` scrape 추가
   - Flink JM/TM metrics scrape 추가
   - distributed에서 `kafka-exporter` 서비스 누락 보완 및 scrape 일치화
   - 관련 파일:
     - `infra/prometheus/prometheus.yml.tmpl`
     - `infra/prometheus/prometheus.distributed.yml`
     - `infra/prometheus/docker-compose.distributed.yml`
4. Kafka broker JMX + JMX exporter 병행 구성 추가
   - broker JMX remote 활성화(`KAFKA_JMX_*`)
   - `jmx-exporter-kafka-{1,2,3}` 사이드카 추가
   - Prometheus `kafka-jmx` scrape job 추가(single/distributed)
   - 관련 파일:
     - `infra/kafka/docker-compose.yml`
     - `infra/kafka/docker-compose.distributed.yml`
     - `infra/kafka/jmx-exporter/kafka-kraft-3_0_0-rules.yml`
     - `infra/prometheus/prometheus.yml.tmpl`
     - `infra/prometheus/prometheus.distributed.yml`
5. 설정 파일 검증 완료
   - `docker compose ... config` (kafka/clickhouse/flink/prometheus) 통과
   - `promtool check config` (single/distributed prometheus config) 통과
6. Ingestor Actuator + Prometheus endpoint 활성화
   - `spring-boot-starter-actuator`, `micrometer-registry-prometheus` 추가
   - `/actuator/prometheus`, `/actuator/health` 노출
   - ingestor 커스텀 metric(counter/gauge/timer) 연결
   - Docker build(`services/ingestor`) 컴파일/패키징 성공 확인
   - 런타임 `/actuator/prometheus` 응답(HTTP 200) 및 `ingestor_*` 메트릭 노출 확인
   - 관련 파일:
     - `services/ingestor/build.gradle`
     - `services/ingestor/src/main/resources/application.yml`
     - `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java`
     - `services/ingestor/src/main/java/com/ingestion/service/DeadLetterQueue.java`
     - `docs/logging/ingestor-logging.md`
7. Nginx exporter(`stub_status`) 도입 및 Prometheus scrape 정식화
   - `nginx-prometheus-exporter` 서비스 추가(single/distributed)
   - nginx `stub_status` endpoint(`/nginx_status`) 추가
   - Prometheus nginx scrape target을 exporter 기준으로 변경
   - 런타임 `/metrics` 응답(HTTP 200) 및 `nginx_*` 메트릭 노출 확인
   - 관련 파일:
     - `infra/nginx/nginx.single-machine.conf`
     - `infra/nginx/templates/nginx.distributed.conf.template`
     - `infra/nginx/docker-compose.yml`
     - `infra/nginx/docker-compose.distributed.yml`
     - `infra/prometheus/prometheus.yml.tmpl`
     - `infra/prometheus/prometheus.distributed.yml`
     - `docs/logging/nginx-logging.md`
8. `docs/logging` 정석 매핑 섹션 고정
   - Generator: native `/metrics` 명세 + Loki triage 범위 명시
   - Flink: reporter endpoint/scrape 경로 + metric 축 + Loki triage 범위 명시
   - ClickHouse: native `/metrics` + SQL 보완 축 + Loki triage 범위 명시
   - Kafka: `kafka-exporter`/`kafka-jmx` dual-run 역할 분담 + Loki triage 범위 명시
   - 관련 파일:
     - `docs/logging/generator-logging.md`
     - `docs/logging/flink-logging.md`
     - `docs/logging/clickhouse-logging.md`
     - `docs/logging/kafka-logging.md`
9. Generator native Prometheus `/metrics` 구현
   - `services/generator/generate.cpp`에 metrics endpoint + counter/gauge/histogram 노출 추가
   - `GENERATOR_METRICS_PORT`(기본 `9108`) 환경변수/설정 파일 연동
   - 관련 파일:
     - `services/generator/generate.cpp`
     - `services/generator/config/default.yaml`
     - `services/generator/README.md`
10. Prometheus generator scrape + rule 파일 로딩 경로 추가
    - `generator` scrape job(single/distributed) 추가
    - `rule_files: /etc/prometheus/rules/*.yml` 추가
    - entrypoint 치환 토큰 보강(`GENERATOR_METRICS_PORT`, `*_METRICS_PORT`, `*_EXPORTER_PORT`, `*_JMX_EXPORTER_PORT`)
    - 관련 파일:
      - `infra/prometheus/prometheus.yml.tmpl`
      - `infra/prometheus/prometheus.distributed.yml`
      - `infra/prometheus/entrypoint.sh`
      - `infra/prometheus/docker-compose.yml`
      - `infra/prometheus/docker-compose.distributed.yml`
11. Loki generator file scrape 경로 추가
    - promtail `generator-file` job 추가
    - promtail 컨테이너에 generator data 볼륨 마운트 추가
    - 관련 파일:
      - `infra/loki/promtail-config.yml`
      - `infra/loki/promtail-config.distributed.yml`
      - `infra/loki/docker-compose.yml`
      - `infra/loki/docker-compose.distributed.yml`
12. Prometheus recording/alert rules 초안 추가
    - 공통 recording rule + component alert rule 추가
    - 관련 파일:
      - `infra/prometheus/rules/recording.rules.yml`
      - `infra/prometheus/rules/alerts.rules.yml`
13. Kafka dual-run 의미축/실 metric 매핑표 고정
    - `kafka-exporter` vs `kafka-jmx`의 1차/2차 소스 기준 명시
    - dual-run 종료 판단 기준 명시
    - 관련 파일:
      - `docs/logging/kafka-logging.md`
14. Loki promtail 공통 파싱 라벨(`tag/level/type/stage`) 반영
    - docker + generator-file scrape에 pipeline_stages 추가
    - 관련 파일:
      - `infra/loki/promtail-config.yml`
      - `infra/loki/promtail-config.distributed.yml`
15. `docs/logging` 기준 Prometheus rule 대량 반영 (전 컴포넌트)
    - component별 recording/alert rule 확장:
      - generator / ingestor / kafka / nginx / flink / clickhouse
    - `docs/logging/*.md` 임계치/의미축을 rules로 1차 매핑
    - 검증:
      - `recording.rules.yml` 95 rules (`promtool check rules` 성공)
      - `alerts.rules.yml` 105 rules (`promtool check rules` 성공)
      - `prometheus.distributed.yml`, `prometheus.yml.tmpl` config syntax 성공
    - 관련 파일:
      - `infra/prometheus/rules/recording.rules.yml`
      - `infra/prometheus/rules/alerts.rules.yml`
16. Nginx log-derived metric source 보강 (Promtail metrics)
    - promtail pipeline에서 `ingestor-lb` access/error 로그를 지표로 승격:
      - `nginx_request_total`, `nginx_status_5xx_total`, `nginx_status_499_total`
      - `nginx_retry_invoked_total`, `nginx_retry_success_total`, `nginx_retry_exhausted_total`
      - `nginx_timeout_{connect,send,read}_total`
      - `nginx_upstream_{1,2,3}_requests_total`
      - `nginx_request_time_seconds_*`, `nginx_upstream_response_time_seconds_*`
    - Prometheus `promtail-loki` scrape(분산: `${LOKI_IP}:${PROMTAIL_LOKI_PORT:-9084}`)로 수집 연결
    - runtime 확인:
      - `up{job="promtail-loki",instance="${LOKI_IP}:${PROMTAIL_LOKI_PORT:-9084}"} = 1`
      - 임시 `ingestor-lb` 로그 주입 후 `nginx_request_total` 수집 확인
    - 관련 파일:
      - `infra/loki/promtail-config.yml`
      - `infra/loki/promtail-config.distributed.yml`
      - `infra/loki/docker-compose.yml`
      - `infra/loki/docker-compose.distributed.yml`
      - `infra/prometheus/prometheus.yml.tmpl`
      - `infra/prometheus/prometheus.distributed.yml`
17. Fire-drill(알림 동작) 1차 점검
    - Prometheus `/api/v1/alerts`에서 `JobDown*` 계열이 예상대로 `firing` 전이됨을 확인
    - Prometheus `/api/v1/rules`에서 `observability_alerts_*` / `observability_recording_*` 그룹 로딩/평가 확인
    - 현재 로컬은 다수 컴포넌트 미기동 상태라 `JobDown*` 다발 firing은 정상 동작 결과로 해석

보류/다음 작업:
1. Ingestor DLQ 파일 경로 권한 정리
   - 증상: `/app/data/dead_letter_queue-ingestor-1.jsonl (Permission denied)`로 컨테이너 부팅 실패 가능
   - 현재: `/tmp` 폴백 적용으로 부팅/metrics 노출은 가능
   - 조치: host volume 권한/소유권 정리 또는 writable 경로 정책 통일
2. 런타임 통합 검증 보강
   - 현재: generator 경로 + promtail-loki 경로(nginx log-derived metrics) 검증 완료
   - 조치:
     - 전체 job(`generator/ingestor/kafka/kafka-jmx/flink/nginx/clickhouse`) `up==1` 스냅샷 고정
     - 주요 alert rule fire drill(1~2개) 수행 후 annotation/triage 링크 점검
     - 분산 환경(`config/.env`) 기준 IP/port 매핑 재확인
3. Loki 파싱 고도화 및 알림 연계
   - 현재: 공통 라벨(`tag/level/type/stage`) + Nginx upstream/timeout/retry 지표 추출까지 반영 완료
   - 다음: triage 전용 LogQL 템플릿과 Prometheus alert annotation 링크 고정
4. Kafka dual-run 축소 의사결정
   - 현재: 의미축/실 metric 매핑표는 고정 완료
   - 다음: 2주 운영 데이터 기반으로 중복 지표/운영비용 비교 후 최종 유지 조합 결정
5. alert threshold 튜닝
   - 현재 `infra/prometheus/rules/*.yml`은 운영 시작용 초안
   - 1주 baseline 관측 후 threshold/for/window 조정 필요
6. 커스텀 metric source 보강
   - 상태: rule에는 logging 문서 기준으로 포함했으며, 일부 metric은 아직 원천 exporter/collector가 없음
   - 대표 대상:
     - clickhouse freshness/dedup/query-class 계열(SQL 집계 exporter 필요)
     - flink e2e/data_balance/onnx 계열(custom metric 노출 필요)

상태 요약:
- ClickHouse/Flink/Ingestor/Nginx는 "공식 metric 경로" 적용 시작 완료
- Generator는 Prometheus 정석안(native `/metrics`) 구현 및 기본 런타임 검증 완료
- Kafka는 `kafka-exporter` + `kafka-jmx` 병행 수집 상태
- Prometheus recording/alert rules를 `docs/logging` 기준으로 1차 대량 반영 완료
- Loki 표준화/파싱 고도화는 다음 단계

### 1.4 정석 적용 고정안 (2026-02-22)
컴포넌트별 1차/2차 관측 책임을 아래로 고정한다.

1. Generator
   - Prometheus(1차): native `GET /metrics` (active)
   - Loki(2차): `429/5xx/socket-error/requeue/dlq/cb_transition` 원인 로그
2. Nginx
   - Prometheus(1차): `nginx-prometheus-exporter`(`stub_status`) + `promtail` log-derived metrics
   - Loki(2차): access/error 로그 기반 timeout/retry 경로 분석
3. Ingestor
   - Prometheus(1차): `GET /actuator/prometheus` (active)
   - Loki(2차): overflow/non-serialized/retry-exhausted/DLQ 실패 이벤트
4. Kafka
   - Prometheus(1차): `kafka-exporter` + `kafka-jmx` dual-run (active)
   - Loki(2차): broker/controller 에러 패턴
5. Flink
   - Prometheus(1차): Flink PrometheusReporter (`:9249`) (active)
   - Loki(2차): deserialization/late-drop/DLQ/sink-failure 샘플
6. ClickHouse
   - Prometheus(1차): ClickHouse native `/metrics` (`:9363`) (active)
   - Loki(2차): insert reject/mutation failure/query anomaly 이벤트

### 1.5 런타임 검증 스냅샷 (2026-02-22)
이번 사이클에서 실제 실행 검증한 항목:

1. Generator `/metrics` 노출 확인
   - 명령: `GENERATOR_METRICS_PORT=9108 ./build/generate ...`
   - 확인: `curl http://127.0.0.1:9108/metrics` 응답에서 `generator_*` 지표 노출 확인
2. Prometheus 설정/룰 로딩 확인
   - `promtool check config` (single/distributed) 통과
   - `promtool check rules` 통과 (`recording.rules.yml`, `alerts.rules.yml`)
   - `/api/v1/rules`에 `observability_alerts`, `observability_recording` 그룹 로드 확인
3. Prometheus scrape 확인
   - `up{job="generator"} = 1` 확인
   - `generator_payload_queue_utilization_ratio`, `generator_http_socket_errors_total` 쿼리 응답 확인
4. Loki ingest 확인
   - `label job values`에 `generator-file` 존재
   - LogQL `{job="generator-file"}` 조회 결과 1개 이상 확인

환경 주의:
- 현재 로컬 검증에서는 `GENERATOR_IP=127.0.0.1` 오버라이드로 Prometheus를 재기동해 generator target을 맞췄다.
- 운영 분산 환경에서는 `config/.env`의 `GENERATOR_IP`를 실제 generator 실행 호스트 IP로 유지해야 한다.

---

## 2) 현재 구조 요약 (As-Is)

파이프라인:
`generator -> nginx -> ingestor -> kafka -> flink -> clickhouse`

현재 관측 스택:
- Prometheus: `infra/prometheus/*`
- Loki/Promtail: `infra/loki/*` + 컴포넌트별 `promtail-config.yml`
- Grafana: `infra/grafana/*`

현재 상태 진단:
1. Loki 로그 수집은 컨테이너 로그 중심으로 이미 배치됨.
2. Prometheus는 `generator` + `kafka-exporter` + `kafka-jmx` + `ingestor` + `nginx` + `clickhouse` + `flink` + blackbox로 구성됨.
3. 핵심 메트릭 경로는 연결 완료이며, 고도화 항목은 alert threshold/LogQL 파싱 표준화에 집중된다.

---

## 3) 갭 분석 (핵심)

주의:
- 아래 갭 분석은 착수 시점 baseline이다.
- 최신 반영 상태는 `1.3 현재 적용 현황`을 우선 기준으로 본다.

### 3.1 Prometheus 갭
1. Generator:
   - native `/metrics` + scrape 반영 완료.
   - 다음 단계: 분산 환경 IP/port 고정 정책 및 alert threshold 튜닝.
2. Ingestor:
   - `/actuator/prometheus` 활성화 완료.
   - 다음 단계: `docs/logging/ingestor-logging.md` 임계치 기준 recording/alert rule 매핑.
3. ClickHouse:
   - native `/metrics` 활성화 완료.
   - 다음 단계: `parts/freshness/final_ratio/read_amp`를 recording rule로 고정.
4. Nginx:
   - `stub_status` + exporter 기반 `/metrics` 경로 활성화 완료.
   - `promtail` log-derived metric으로 retry/timeout/upstream 분해 지표 활성화 완료.
   - 다음 단계: LogQL triage 템플릿/알림 annotation 링크 고정.
5. Flink:
   - PrometheusReporter 활성화 및 포트 노출 완료.
   - 다음 단계: `checkpoint/backpressure/watermark/late_ratio` 지표를 alert rule로 고정.
6. 알림:
   - `infra/prometheus/rules/*.yml` 초안 반영 완료.
   - 다음 단계: `docs/logging` 임계치 기준으로 rule 세분화/튜닝.
7. Kafka dual-run:
   - `kafka-exporter` + `kafka-jmx`는 붙었지만 지표 중복/최종 유지 조합 미확정.

### 3.2 Loki 갭
1. Generator:
   - 파일 scrape(`generator-file`) 추가 완료.
   - 다음 단계: 경보 유형별 LogQL triage 템플릿 고정.
2. Nginx access 로그:
   - stdout/stderr 전환 + `upstream_status/upstream_addr/timeout reason` 기반 metric 추출 반영 완료.
   - 다음 단계: triage용 LogQL 템플릿 표준화.
3. 로그 포맷 표준:
   - `docs/logging`는 `[GEN_METRICS]/[ING_METRICS]/...` 제안,
   - 실제 구현은 서비스별 접두사/필드 포맷이 상이해 파싱 규칙 표준화 필요.
4. promtail 파이프라인:
   - 공통 라벨(`tag/level/type/stage`) 추출은 반영 완료.
   - 다음 단계: 컴포넌트별 세부 필드(`upstream_status`, `action`, `error_code`) 파서 확장.
5. 알림 연계:
   - Loki 기반 triage 쿼리와 Prometheus alert annotations(로그 링크) 연결이 미완.

---

## 4) 목표 구조 (To-Be)

### 4.1 수집 원칙
1. Prometheus:
   - 카운터/게이지/히스토그램 등 장기 추세 지표
   - SLO/경보 대상 지표
2. Loki:
   - 오류 원인, 예외 샘플, 상태 전이, 드롭 사유
   - 운영자가 사건 당시 문맥을 재구성하는 데이터
3. 원칙:
   - "측정 가능한 것은 메트릭으로"
   - "설명/원인 분석이 필요한 것은 로그로"
   - 동일 주제에 대해 Prometheus(탐지) + Loki(원인) 2단 구성을 기본으로 한다.

### 4.2 서비스별 책임 분리
1. Generator:
   - Prometheus: batch throughput, status code rate, queue depth, requeue/dlq 카운터
   - Loki: 배치/429/CB/requeue/DLQ 이벤트 우선
2. Nginx:
   - Prometheus: request rate, status code, upstream latency(nginx exporter)
   - Loki: access/error 원문(재시도 경로 분석)
3. Ingestor:
   - Prometheus: throughput, dropped, inflight, retry
   - Loki: overflow/non-serialized/DLQ write 실패 샘플
4. Kafka:
   - Prometheus: `kafka-exporter`(lag/offset/partition) + `kafka-jmx`(queue/idle/request latency) 병행
   - Loki: broker/controller 에러 패턴
5. Flink:
   - Prometheus: checkpoint/backpressure/operator 지표
   - Loki: deserialization/late-drop/DLQ/ONNX 오류 샘플
6. ClickHouse:
   - Prometheus: server/resource/merge-tree core metric
   - Loki: insert reject/mutation failure/query anomaly 이벤트

### 4.3 End-to-End Latency 관측 축
컴포넌트 간 홉별 지연을 분리 관측한다.

1. `generator -> nginx`
   - Prometheus: client request latency histogram, HTTP status rate
   - Loki: timeout/socket error 샘플
2. `nginx -> ingestor(upstream)`
   - Prometheus: upstream latency(p50/p95), retry ratio, upstream error ratio
   - Loki: `upstream_status`, timeout 원인(connect/send/read)
3. `ingestor -> kafka`
   - Prometheus: batch send latency, inflight, retry/exhausted
   - Loki: 배치 실패/재시도 상세
4. `kafka -> flink`
   - Prometheus: consumer lag, lag recovery rate, source records/s
   - Loki: deserialization/validation/drop 원인
5. `flink -> clickhouse`
   - Prometheus: JDBC flush latency, retry/error, rows out
   - Loki: sink failure/late-drop/DLQ 상세
6. `clickhouse query path`
   - Prometheus: query latency p50/p95, read_rows/read_bytes, FINAL ratio
   - Loki: query 예외/insert reject/mutation 실패

운영 목표:
1. 홉별 `p50/p95`를 같은 타임라인에서 비교한다.
2. 특정 홉 지연 급등 시 바로 이전/다음 홉과 함께 비교해 병목 위치를 확정한다.

---

## 5) 단계별 구현 계획

## Phase 0: 기준선 정리 (MVP 준비)
목표: "수집 경로가 실제로 열려 있는 상태"를 먼저 만든다.

작업:
1. Prometheus 스크랩 타겟 현실화 (완료, generator 포함).
2. Loki ingestion 상태 점검 (`job`, `container`, `stream` 라벨 일관성) (진행중).
3. Generator 로그 파일 수집 경로 추가(네이티브 실행 대응) (완료).

완료 기준:
1. Prometheus target DOWN이 설계상 의도된 항목 제외하고 0에 수렴.
2. Loki에서 `generator|ingestor|nginx|flink|kafka|clickhouse` 로그가 모두 조회됨.

## Phase 1: 메트릭 엔드포인트 구현
목표: `docs/logging`의 핵심 정량 지표를 Prometheus로 올린다.

작업:
1. Generator:
   - 정석안(native `/metrics`) 적용 (완료)
   - `events/sec`, status code, queue depth, retry/dlq, CB/rate-limit 핵심 지표 노출 (완료)
   - Prometheus scrape(`generator` job) single/distributed 반영 (완료)
2. Ingestor:
   - actuator + micrometer(prometheus) 활성화 (완료)
   - 다음 단계: `events_received/processed/failed/dropped`, batch/retry/inflight alert 고정
3. ClickHouse:
   - `config.xml` prometheus endpoint 설정(9363) 추가 (완료)
   - 다음 단계: part/freshness/query 관련 recording rule 고정
4. Nginx:
   - `stub_status` + nginx exporter 도입 후 스크랩 (완료)
   - 다음 단계: timeout/retry 원인 분석용 LogQL 표준화
5. Flink:
   - PrometheusReporter 활성화 + scrape 가능 포트 노출 (완료)
   - 다음 단계: operator/checkpoint/watermark alert 고정
6. Kafka:
   - `kafka-exporter` + broker JMX exporter 동시 수집
   - metric 의미 축 기준(URP/lag/queue_pressure/request_latency) 매핑표 정리
7. E2E latency metric 표준화:
   - 홉별 latency metric name/p50/p95 패널 정의
   - 대시보드에서 `generator->nginx->ingestor->kafka->flink->clickhouse` 순서로 연속 비교 가능하게 구성

완료 기준:
1. Generator 핵심 지표가 Prometheus에서 조회 가능해야 한다.
2. `generator` job이 Prometheus에서 `UP`.
3. `ingestor`, `clickhouse`, `nginx`, `flink`, `kafka`, `kafka-jmx` job이 Prometheus에서 `UP`.
4. 각 서비스 기본 처리량/지연/오류율 패널이 그래프화됨.
5. 홉별 E2E latency(`p50/p95`) 패널이 구성되어 병목 구간 식별이 가능함.

## Phase 2: 구조화 로그 표준화 + Loki 파싱
목표: 경보 시 원인 분석까지 1~2분 내 가능하게 한다.

작업:
1. 로그 접두사/키 표준:
   - `[GEN_METRICS]`, `[ING_METRICS]`, `[FLK_WARN]`, `[CH_ERROR]` 형태 정규화
2. Nginx access 로그 stdout 전환:
   - access/error를 Loki 수집 가능한 경로로 변경
3. promtail pipeline_stages 추가:
   - level/tag/type/action 추출
   - key=value 파싱(logfmt 또는 regex)
4. 홉별 지연 이상 시 원인 추적 로그 라벨 정리:
   - `container`, `level`, `type`, `stage` 기준으로 지연 구간별 drill-down 가능하게 표준화

완료 기준:
1. Loki에서 `level`, `tag`, `container` 필터로 즉시 drill-down 가능.
2. timeout/retry/dlq/late-drop 원인이 로그 쿼리 1~2개로 분리됨.
3. 홉별 지연 경보 발생 시 해당 홉 원인 로그를 1분 내 조회 가능함.

## Phase 3: 경보 규칙/대시보드 고도화
목표: `docs/logging` 임계치를 실제 운영 경보로 연결.

작업:
1. Prometheus recording/alert rules 작성
2. Grafana dashboard 분리:
   - Pipeline health
   - Ingest path
   - Stream processing
   - Storage(ClickHouse)
   - Logs triage
3. 경보 기준 반영:
   - `active_parts_total`, `latest_dup_groups_*`, `insert_error_delta`, `q_p95_latest_ms`, `disk_used_pct`
   - `operator_backpressured_time_pct`, `checkpoint_age`, `dlq_ratio` 등
4. E2E latency budget alert 추가:
   - 홉별 `p95` 임계 초과 지속 시 `WARN/ERROR`
   - 병목 홉 자동 식별을 위한 패널/룰 연계

완료 기준:
1. 경보 1건 발생 시 대시보드 + 로그 drill-down 링크로 원인 추적 가능.
2. 문서 임계치와 실제 alert rule이 1:1 매핑됨.
3. 네트워크 지연 증가 시 어느 홉에서 latency가 확대되는지 대시보드에서 즉시 확인 가능.

---

## 6) 파일 단위 변경 백로그 (우선순위)

P0 (먼저):
1. `docs/logging/kafka-logging.md` (dual-run 의미축/실 metric 매핑표 고정)
2. `infra/prometheus/rules/recording.rules.yml` (Flink/ClickHouse/Kafka 고도화 recording)
3. `infra/prometheus/rules/alerts.rules.yml` (`docs/logging` 임계치 기준 세분화)
4. `infra/loki/promtail-config.yml` (서비스별 상세 필드 파서 확장)
5. `infra/loki/promtail-config.distributed.yml` (single과 동일 파싱 정책 동기화)
6. `docs/logging/flink-logging.md` (PromQL/LogQL 검증 쿼리 섹션 고정)
7. `docs/logging/clickhouse-logging.md` (freshness/parts/query guard 쿼리 고정)
8. `docs/current/current-observability-prometheus-loki-implementation-plan-2026-02-21.md` (사이클 완료 상태 업데이트)

P1 (다음):
1. `infra/grafana/provisioning/dashboards/*` 신규/개편
2. `docs/logging/*`의 metric name을 실제 구현명으로 고정
3. `docs/current/*`에서 단계 완료 상태 주기 동기화

---

## 7) 검증 시나리오 (구현 후)

1. 정상 부하:
   - Generator 실행 후 `events/sec`, Kafka lag, Flink sink out, ClickHouse row delta가 동행
2. 백프레셔 유도:
   - Generator 속도 상향 -> Ingestor overflow/retry 증가 확인
3. 장애 유도:
   - Ingestor 1개 중지 -> Nginx retry 및 upstream 분배 변화 확인
4. 저장소 압박:
   - ClickHouse part 증가 상황에서 warn/error 임계 동작 확인
5. 네트워크 지연 주입 테스트:
   - 특정 구간(예: nginx->ingestor 또는 ingestor->kafka)에 지연을 주입했을 때,
   - 홉별 latency 패널과 관련 error/retry 로그가 의도대로 변하는지 확인

성공 판정:
1. Prometheus 알림 + Loki 원인 로그가 같은 시간축으로 교차 검증된다.
2. `docs/logging` 체크리스트 항목을 Grafana/Loki에서 직접 확인 가능하다.
3. E2E 홉별 latency 비교로 병목 구간을 단일 구간으로 수렴시킬 수 있다.

---

## 8) 이번 문서 기준 즉시 실행 순서 (추천)

1. Kafka dual-run(`kafka-exporter` + `kafka-jmx`) 지표 매핑표 확정
2. Flink/ClickHouse/Kafka alert/recording rule를 `docs/logging` 임계치 기준으로 세분화
3. Loki 서비스별 파서 확장(`upstream_status`, timeout reason 등) + triage LogQL 고정
4. 통합 런타임 검증(`up==1`, rule/alert, LogQL ingest) 재실행
5. Grafana 홉별 E2E latency(p50/p95) 패널 구성

이 순서로 진행하면, 이미 열린 공식 metric 경로를 유지한 채 generator를 포함한 전 구간을 Prometheus 1차 관측으로 정렬할 수 있다.
