# Current Logging Implementation (코드 기준)

## 1. 조사 범위 / 방법
- 기준 시점: 현재 워크스페이스 소스 코드
- 조사 경로: `services/`, `infra/`, `data/`, `main.py`
- 제외: 빌드 산출물(`build/`, `target/`, `.gradle/`)과 문서성 파일(`*.md`)의 설명 텍스트
- 탐색 키워드: `log.info|warn|error|debug`, `@Slf4j`, `System.out|System.err`, `cout|cerr`, `print(`, `access_log`, `promtail`, `loki`

## 2. 로깅 구조 요약
- `generator` (C++): 로깅 프레임워크 없이 `stdout/stderr` 직접 출력 + DLQ 파일 기록.
- `ingestor` (Spring Boot): `Slf4j` 기반 애플리케이션 로그 + `System.out` shutdown 로그 + DLQ 파일(JSONL) 기록.
- `flink-job` (Java/Flink): `System.out/err` + `.print()` sink 기반 stdout 로그.
- `nginx` (LB): `access_log` 포맷 지정, upstream/latency 포함.
- 수집 파이프라인: `promtail`이 Docker 로그를 수집해 `loki`로 전송, Grafana 대시보드(`Pipeline Logs`)에서 조회.

## 3. 서비스별 상세

### 3.1 Generator (`services/generator/generate.cpp`)
출력 방식
- `stdout`: 운영 상태/메트릭/디버그/네트워크 payload fallback
- `stderr`: 설정 경고, 레이트리미트/서킷브레이커 상태 전이, DLQ/배치 오류
- 파일: DLQ (`dead_letter_queue.jsonl`, `timestamp|retry_count|payload` 포맷)

구현 근거 (핵심 로그)
- 설정/초기화
  - `"[Config] Using defaults..."` (`services/generator/generate.cpp:461`)
  - `"[Config] Invalid ingestion_url; falling back to stdout."` (`services/generator/generate.cpp:1393`)
  - 시작 배너/큐 개수/첫 이벤트 시각 (`services/generator/generate.cpp:1425`, `services/generator/generate.cpp:1426`, `services/generator/generate.cpp:1429`, `services/generator/generate.cpp:1430`)
- 주기 메트릭(10초)
  - `"[METRICS] rate_limit_delay=..., 429_rate=..., circuit_state=..., dlq_writes=..., batches_sent=..."` (`services/generator/generate.cpp:1459`)
- 레이트리미터/서킷브레이커 이벤트
  - `"[RATE_LIMIT] Increased/Decreased delay..."` (`services/generator/generate.cpp:921`, `services/generator/generate.cpp:935`)
  - `"[CIRCUIT_BREAKER] CLOSED->OPEN / OPEN->HALF_OPEN / HALF_OPEN->..."` (`services/generator/generate.cpp:1092`, `services/generator/generate.cpp:1102`, `services/generator/generate.cpp:1113`, `services/generator/generate.cpp:1121`)
- DLQ/재시도/배치 오류
  - `"[DLQ] Failed to open dead-letter file"` (`services/generator/generate.cpp:1303`)
  - `"[DLQ] ... wrote to DLQ"` (`services/generator/generate.cpp:1807`, `services/generator/generate.cpp:1813`, `services/generator/generate.cpp:1849`, `services/generator/generate.cpp:1855`)
  - `"[BATCH] 400 on batch..."`, `"[BATCH] Client error on batch..."` (`services/generator/generate.cpp:1928`, `services/generator/generate.cpp:1951`)
- payload 출력(ingestion_url 파싱 실패 시 fallback)
  - `"[NET] <json payload>"` (`services/generator/generate.cpp:1826`, `services/generator/generate.cpp:1902`)
- 로더 단계
  - 파일 읽기/타임스탬프 샘플/스키마 누락/파케이 오류 (`services/generator/generate.cpp:1981`, `services/generator/generate.cpp:2005`, `services/generator/generate.cpp:2129`, `services/generator/generate.cpp:2251`, `services/generator/generate.cpp:2330`)
- 종료 요약
  - Events sent, CB trips/rejects, 429s, DLQ writes, batch 통계 (`services/generator/generate.cpp:1594` ~ `services/generator/generate.cpp:1607`)

주의사항 (현재 코드 상태)
- `"[DEBUG]"` 로그가 다수 상시 출력됨(예: `services/generator/generate.cpp:1432`, `services/generator/generate.cpp:1485`, `services/generator/generate.cpp:1633`).
- `debug_timing` 플래그는 일부 디버그 로그만 제어함 (`services/generator/generate.cpp:1704`, `services/generator/generate.cpp:1733`).

설정 근거
- `debug_timing`, `rate_limit`, `circuit_breaker`, `retry.max_retries`, `dlq.filepath`, `batch.*` (`services/generator/config/default.yaml:13`, `services/generator/config/default.yaml:15`, `services/generator/config/default.yaml:19`, `services/generator/config/default.yaml:27`, `services/generator/config/default.yaml:30`, `services/generator/config/default.yaml:33`)

### 3.2 Ingestor (`services/ingestor`)
출력 방식
- `Slf4j` (`@Slf4j`) 기반 structured tag 로그
- `System.out` shutdown hook 로그
- DLQ 파일(`dead_letter_queue.jsonl`) JSON line 기록

구현 근거 (핵심 로그)
- 컨트롤러 레벨
  - 단건/배치 백프레셔/부분실패/전체실패: `[BACKPRESSURE]`, `[BATCH_BACKPRESSURE]`, `[BATCH_PARTIAL]`, `[BATCH_FAILED]`
  - (`services/ingestor/src/main/java/com/ingestion/controller/IngestionController.java:55`, `services/ingestor/src/main/java/com/ingestion/controller/IngestionController.java:106`, `services/ingestor/src/main/java/com/ingestion/controller/IngestionController.java:126`, `services/ingestor/src/main/java/com/ingestion/controller/IngestionController.java:136`)
- 서비스 레벨
  - 시작/종료/메트릭: `[STARTUP]`, `[SHUTDOWN]`, `[METRICS]`
  - 파이프라인/직렬화/Kafka/재시도: `[PIPELINE]`, `[SERIALIZATION]`, `[KAFKA]`, `[RETRY]`
  - (`services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:79`, `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:86`, `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:132`, `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:155`, `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:238`, `services/ingestor/src/main/java/com/ingestion/service/IngestionService.java:249`)
- DLQ 클래스
  - 초기화/쓰기/종료: `[DLQ] ... initialized`, `[DLQ] ... written`, `[DLQ] ... closed`
  - (`services/ingestor/src/main/java/com/ingestion/service/DeadLetterQueue.java:22`, `services/ingestor/src/main/java/com/ingestion/service/DeadLetterQueue.java:46`, `services/ingestor/src/main/java/com/ingestion/service/DeadLetterQueue.java:61`)
- JVM 종료 훅
  - `"[SHUTDOWN] Graceful shutdown initiated..."`, `"[SHUTDOWN] Application stopped"` (`services/ingestor/src/main/java/com/ingestion/IngestionApplication.java:17`, `services/ingestor/src/main/java/com/ingestion/IngestionApplication.java:19`)

설정 근거
- DLQ 경로 기본값: `app.dlq.filepath` (`services/ingestor/src/main/resources/application.yml:4` ~ `services/ingestor/src/main/resources/application.yml:6`)
- 메트릭 주기/버퍼/배치/병렬성: compose env (`services/ingestor/docker-compose.yml:13` ~ `services/ingestor/docker-compose.yml:17`, `services/ingestor/docker-compose.distributed.yml:11` ~ `services/ingestor/docker-compose.distributed.yml:15`)

참고
- `application.yml`에 `logging.level.*` 오버라이드는 없음(스프링 기본 로깅 레벨 체계 사용).

### 3.3 Flink Job (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java`)
출력 방식
- `System.out`/`System.err`
- Flink `.print()` sink로 레코드 단위 stdout 출력

구현 근거
- 시작 설정 출력: `[CONFIG] ...` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:73`)
- Late-drop 출력 코드는 존재하지만 기본 비활성(주석 처리): `orderedPerTrip.getSideOutput(LATE_EVENTS)...print()` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:123` ~ `services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:126`)
- ClickHouse sink 비활성 경고: `[WARN] ...` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:152`)
- 수요집계 결과 `.print()` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:160`)
- 역직렬화 스키마 로그
  - 초기화: `[SafeTaxiEventSchema] Initialized` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:416`)
  - 오류 샘플: `[DESER_ERROR] ...` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:428`)
- 수요 row 문자열: `[ML_DEMAND] Zone: ..., Time: ..., Count: ...` (`services/flink-job/src/main/java/com/example/TaxiRealtimeJob.java:384`)

### 3.4 Nginx LB (`infra/nginx`)
출력 방식
- `access_log /var/log/nginx/access.log upstream_log`
- `upstream_log` 포맷에 upstream 라우팅/상태/지연 포함

구현 근거
- 로그 포맷/경로
  - `log_format upstream_log ... upstream_addr ... upstream_status ... request_time ... upstream_response_time ...`
  - (`infra/nginx/nginx.single-machine.conf:6`, `infra/nginx/nginx.single-machine.conf:10`)
  - (`infra/nginx/templates/nginx.distributed.conf.template:6`, `infra/nginx/templates/nginx.distributed.conf.template:10`)
- 재시도/타임아웃 관련 동작
  - `proxy_connect_timeout`, `proxy_send_timeout`, `proxy_read_timeout`
  - `proxy_next_upstream error timeout http_502 http_503 http_504`
  - `proxy_next_upstream_tries 2`
  - (`infra/nginx/nginx.single-machine.conf:32` ~ `infra/nginx/nginx.single-machine.conf:36`, `infra/nginx/templates/nginx.distributed.conf.template:31` ~ `infra/nginx/templates/nginx.distributed.conf.template:35`)
- 버퍼 관련 설정
  - `proxy_buffering on`, `proxy_buffer_size 8k`, `proxy_buffers 16 8k`, `proxy_busy_buffers_size 16k`
  - (`infra/nginx/nginx.single-machine.conf:37` ~ `infra/nginx/nginx.single-machine.conf:40`)

참고
- `error_log`는 이 repo 설정에서 별도 오버라이드하지 않음.

### 3.5 GeoJSON Updater (`infra/grafana/scripts/update_geojson.py`)
출력 방식
- `print()` 기반 상태/경고 출력 (컨테이너 stdout/stderr)

구현 근거
- 시작/환경/주기 상태: `[INFO] Starting...`, `[INFO] ClickHouse...`, `[INFO] Updated GeoJSON...`
  - (`infra/grafana/scripts/update_geojson.py:134`, `infra/grafana/scripts/update_geojson.py:135`, `infra/grafana/scripts/update_geojson.py:130`)
- 조회 실패 경고: `[WARN] ClickHouse query failed: ...` (`infra/grafana/scripts/update_geojson.py:85`)

## 4. 로그 수집 / 조회 파이프라인

### 4.1 로컬 수집 (`infra/loki`)
- Loki/Promtail 컨테이너 구성 (`infra/loki/docker-compose.yml:4`, `infra/loki/docker-compose.yml:16`)
- Promtail이 Docker SD로 컨테이너 로그 수집 후 Loki push
  - `job_name: docker-containers` (`infra/loki/promtail-config.yml:12`)
  - `url: http://loki:3100/loki/api/v1/push` (`infra/loki/promtail-config.yml:9`)
  - 수집 대상 regex 필터(ingestor/nginx/flink/kafka 등) (`infra/loki/promtail-config.yml:18`)
  - `job=docker`, `container`, `container_id` 라벨 부여 (`infra/loki/promtail-config.yml:21`, `infra/loki/promtail-config.yml:23`)

### 4.2 원격 수집 (`infra/promtail-remote`)
- 원격 Promtail push URL: `${LOKI_URL}/loki/api/v1/push` (`infra/promtail-remote/promtail-config.yml:9`)
- Docker daemon Loki driver 설정 스크립트
  - `log-driver: loki`, `loki-url`, `loki-batch-size`, `loki-external-labels`
  - (`infra/promtail-remote/setup.sh:37`, `infra/promtail-remote/setup.sh:39`, `infra/promtail-remote/setup.sh:40`, `infra/promtail-remote/setup.sh:41`)

### 4.3 Grafana 조회
- Loki datasource 등록 (`infra/grafana/provisioning/datasources/clickhouse.yml:22`, `infra/grafana/provisioning/datasources/clickhouse.yml:26`)
- 로그 대시보드 쿼리
  - 전체 에러 필터: `{job="docker"} |~ "(?i)(error|exception|fail|fatal)"` (`infra/grafana/provisioning/dashboards/loki-logs.json:38`)
  - 인게스터/Nginx/Flink 컨테이너 로그 패널 (`infra/grafana/provisioning/dashboards/loki-logs.json:101`, `infra/grafana/provisioning/dashboards/loki-logs.json:122`, `infra/grafana/provisioning/dashboards/loki-logs.json:80`)

## 5. 운영 파이프라인 외 출력(참고)
- 데이터 전처리/EDA 스크립트들도 `print()` 출력 사용:
  - `data/preprocess/preprocess_taxi_data.py:165`
  - `data/preprocess/preprocess_add_trip_id.py:76`
  - `data/preprocess/preprocess_taxi_zone.py:89`
  - `data/analysis/eda.py:160`
- 루트 엔트리 파일 샘플 출력: `main.py:2`

## 6. 결론 (현재 구현 상태)
- 현재 로깅은 컴포넌트별로 혼합형(`Slf4j`, `stdout/stderr`, 파일 DLQ)이며, 중앙 수집은 Loki/Promtail 경로로 구성됨.
- `generator`는 디버그 출력량이 크고(상시 `[DEBUG]`), `ingestor`는 태그 기반 구조화가 비교적 잘 되어 있음.
- Nginx는 access log 포맷이 upstream 분석에 필요한 필드를 이미 포함하고 있어, Loki/Grafana에서 분배율/지연/재시도 관찰이 가능함.
