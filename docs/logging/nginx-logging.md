# Nginx Logging Guide

## 1) 목적
이 문서는 Nginx LB의 라우팅/장애대응 상태를 운영 관점에서 추적하기 위한 로깅 기준을 정의한다.

핵심 목표:
- upstream(ingestor-1/2/3) 분배 비율을 주기적으로 확인
- timeout 발생 시 원인(어느 단계에서 멈췄는지)과 결과(재시도 성공/실패)를 확인
- proxy buffer 관련 병목 신호를 간접 지표로 감지
- `proxy_next_upstream` 재시도 동작이 실제로 어떻게 수행되는지 확인
- 현재 상태(요청량/상태코드/활성 연결)를 10초 단위로 확인
- 병목 위치(연결 지연 vs upstream 처리 지연 vs downstream 전달 지연) 분리
- 성능 품질(p95 지연, 5xx율, retry 성공률)을 임계치 기반으로 경보

---

## 2) 기본 원칙
- `INFO`: 주기 요약 로그(기본 10초)
- `WARN`: 특정 upstream 편중, timeout 급증, retry 급증
- `ERROR`: 모든 upstream 실패로 최종 에러 반환 급증
- `DEBUG`: 샘플링 상세(특정 request 추적 시 한시적으로)

권장:
- 요청 원문(body) 로그 금지
- “접근 로그 집계 + 에러 로그 패턴 + 임계치 경보” 조합 사용
- access/error 로그는 Loki 수집을 위해 stdout/stderr 경로 우선
- request 단위 상관분석을 위해 `request_id` 필드를 항상 포함

---

## 3) 로깅 항목 (요청안 기준)

### 3.1 현재 상태 스냅샷 (기본)
목적:
- "지금 시스템이 정상인가"를 10초 단위로 판단

권장 주기 집계(`INFO`, 10초):
- `req_total_10s`
- `rps` (`req_total_10s / 10`)
- `status_2xx_10s`
- `status_4xx_10s`
- `status_499_10s` (client abort 분리)
- `status_5xx_10s`
- `active_connections`
- `connections_reading`, `connections_writing`, `connections_waiting`

경보(`WARN`):
- `status_5xx_rate` 급증
- `status_499_rate` 급증(업스트림 문제와 분리해서 판단)

### 3.2 주기별 upstream 분배량
목적:
- `random two least_conn` 분산이 실제로 어느 upstream으로 얼마나 갔는지 확인
- 특정 인스턴스 편향/고립 조기 감지

현재 access log에서 활용 가능한 필드:
- `upstream_addr`
- `upstream_status`
- `request_time`
- `upstream_response_time`

권장 주기 집계(`INFO`, 10초):
- `upstream_req_count{addr}`
- `upstream_req_share_pct{addr}`
- `upstream_2xx_count{addr}`
- `upstream_5xx_count{addr}`
- `upstream_avg_rt_ms{addr}` (`upstream_response_time` 기반)

경보(`WARN`):
- 특정 upstream `share_pct`가 비정상적으로 높거나 0에 수렴
- 특정 upstream의 `5xx` 또는 timeout 패턴 급증

### 3.3 timeout 원인 + 결과 로깅
목적:
- timeout이 연결/전송/응답읽기 중 어디서 발생했는지 식별
- 발생 후 재시도 성공 여부까지 확인

원인 추적:
- `error.log` 패턴 확인
  - `while connecting to upstream`
  - `while sending request to upstream`
  - `while reading response header from upstream`

결과 추적:
- `access.log`의 `upstream_status`, `upstream_addr`는 재시도 시 다중 값으로 남을 수 있음
  - 예: `upstream_status="504, 202"`
  - 예: `upstream_addr="ingestor-2:8080, ingestor-1:8080"`

권장 주기 집계(`INFO`, 10초):
- `timeout_connect_count`
- `timeout_send_count`
- `timeout_read_count`
- `retry_after_timeout_success_count`
- `retry_after_timeout_fail_count`

### 3.4 병목 위치 분해 지표 (성능 확인 핵심)
목적:
- 병목이 Nginx 이전/이후 어디에 있는지 빠르게 구분

필수 필드(가능하면 access log에 포함):
- `upstream_connect_time` (upstream TCP 연결 지연)
- `upstream_header_time` (upstream 첫 헤더까지 지연)
- `upstream_response_time` (upstream 응답 완료까지 지연)
- `request_time` (클라이언트까지 포함한 전체 요청 시간)

권장 주기 집계(`INFO`, 10초):
- `upstream_connect_time_ms_p50/p95`
- `upstream_header_time_ms_p50/p95`
- `upstream_response_time_ms_p50/p95`
- `request_time_ms_p50/p95`
- `downstream_gap_ms_p95 = p95(request_time - upstream_response_time)`

해석 가이드:
- `connect_time`만 상승: 네트워크/업스트림 accept 병목 가능성
- `header/response_time` 상승: 업스트림 애플리케이션 처리 지연 가능성
- `downstream_gap` 상승: 느린 클라이언트/전송 병목 가능성

### 3.5 buffer 상태 로깅 (현실적 범위)
목적:
- 느린 downstream(클라이언트) 또는 큰 응답으로 인한 완충 부담 감지

중요 제한:
- OSS Nginx는 `proxy_buffer`의 “실시간 점유율(%)”을 직접 노출하지 않음

권장 대체 지표(`INFO`, 10초):
- `request_time - upstream_response_time` 격차 추이(클라이언트 전달 지연 간접 신호)
- `body_bytes_sent` 분포
- `connections_writing` (stub_status 사용 시)
- Nginx 컨테이너 메모리 사용량

경보(`WARN`):
- `request_time` 대비 `upstream_response_time`가 지속적으로 낮아 “downstream 전달 지연”이 의심될 때
- writing 연결 수와 메모리 사용량 동시 급증

### 3.6 upstream 재시도 로직 상태
목적:
- `proxy_next_upstream` 경로가 얼마나 자주 동작하는지 파악
- 재시도 후 성공/실패 분포 확인

권장 집계:
- `retry_invoked_count` (다중 `upstream_status` 라인 수 기반)
- `retry_success_count` (초기 실패 후 최종 2xx)
- `retry_exhausted_count` (재시도 후도 최종 실패)
- `retry_target_switch_count` (재시도 시 `upstream_addr`가 변경된 요청 수)

메모:
- 요청 단위 다중 시도를 보려면 `upstream_status`/`upstream_addr`의 콤마 포함 라인을 분석한다.

---

## 4) 추가 권장 항목
- access log 확장(권장 -> 실질적으로 필수):
  - `request_id`
  - `upstream_connect_time`
  - `upstream_header_time`
  - `request_length`
  - `bytes_sent`
  - `connection`, `connection_requests`
- error log 레벨/보존 정책 명시:
  - timeout 원인 분석이 가능하도록 rotation + 보존 기간 설정
- 모니터링 연계:
  - Loki/Promtail로 `access.log`, `error.log` 수집
  - 10초 윈도우 집계 패널(분배율, timeout, retry, 최종 상태코드) 구성

---

## 5) 로그 포맷/수집 구현 기준
권장 access log 포맷(예시):
```text
request_id="$request_id" status="$status" method="$request_method" uri="$request_uri" request_time="$request_time" upstream_addr="$upstream_addr" upstream_status="$upstream_status" upstream_connect_time="$upstream_connect_time" upstream_header_time="$upstream_header_time" upstream_response_time="$upstream_response_time" request_length="$request_length" bytes_sent="$bytes_sent" body_bytes_sent="$body_bytes_sent"
```

권장 설정 기준:
- `access_log /dev/stdout upstream_log;`
- `error_log /dev/stderr warn;`
- `location /nginx_status { stub_status; allow <prometheus_or_exporter_ip>; deny all; }`
- 상위 서비스 전달용 헤더: `proxy_set_header X-Request-ID $request_id;`

메모:
- 파일 로그(`/var/log/nginx/*`) 유지 시 promtail file scrape 설정을 별도로 반드시 추가한다.

---

## 5.1 현재 Prometheus 매핑 (2026-02-22)
Nginx는 Prometheus를 2계층으로 수집한다.

1) `stub_status + nginx-prometheus-exporter` (연결/기본 요청량)
- exporter:
  - image: `nginx/nginx-prometheus-exporter:1.4.2`
  - scrape target: `http://ingestor-lb/nginx_status`
  - metrics endpoint: `:9113/metrics`
- Prometheus scrape:
  - single: `nginx-exporter:9113`
  - distributed: `${NGINX_IP}:${NGINX_EXPORTER_PORT:-9113}`
- nginx 설정:
  - `location = /nginx_status { stub_status; ... }`
  - `access_log /dev/stdout upstream_log;`
  - `error_log /dev/stderr warn;`

2) `promtail` log-derived metrics (retry/timeout/upstream 분해)
- source: `ingestor-lb` access/error 로그
- Prometheus scrape:
  - single: `promtail-loki` (`${LOKI_IP}:${PROMTAIL_LOKI_PORT:-9084}`)
  - distributed: `promtail-nginx` (`${NGINX_IP}:${NGINX_PROMTAIL_PORT:-9085}`)
- 생성 metric:
  - `nginx_request_total`, `nginx_status_5xx_total`, `nginx_status_499_total`
  - `nginx_retry_invoked_total`, `nginx_retry_success_total`, `nginx_retry_exhausted_total`
  - `nginx_timeout_connect_total`, `nginx_timeout_send_total`, `nginx_timeout_read_total`
  - `nginx_upstream_1_requests_total`, `nginx_upstream_2_requests_total`, `nginx_upstream_3_requests_total`
  - `nginx_request_time_seconds_*`, `nginx_upstream_response_time_seconds_*`

운영 쿼리 시작점(예시):
- RPS(로그 기반): `sum(rate(nginx_request_total[1m]))`
- 5xx ratio: `sum(rate(nginx_status_5xx_total[1m])) / clamp_min(sum(rate(nginx_request_total[1m])), 1)`
- retry success ratio: `sum(rate(nginx_retry_success_total[5m])) / clamp_min(sum(rate(nginx_retry_invoked_total[5m])), 1)`
- upstream share(ingestor-1): `100 * sum(rate(nginx_upstream_1_requests_total[5m])) / clamp_min(sum(rate(nginx_upstream_1_requests_total[5m]) + rate(nginx_upstream_2_requests_total[5m]) + rate(nginx_upstream_3_requests_total[5m])), 1)`

메모:
- log-derived metric은 매칭되는 로그가 유입되어야 시계열이 생성된다.
- 연결 상태 계열(`nginx_connections_*`)은 exporter, retry/timeout 계열은 promtail metric을 1차 기준으로 사용한다.

---

## 6) 권장 출력 예시
```text
[NGX_METRICS] ts=... win=10s req_total=12450 rps=1245.0 status_2xx=12390 status_4xx=20 status_499=8 status_5xx=32 up1=4120(33.1%) up2=4189(33.6%) up3=4141(33.3%) retry_invoked=182 retry_success=169 retry_exhausted=13 timeout_connect=2 timeout_send=4 timeout_read=21 rt_p95_ms=410 up_resp_p95_ms=270 downstream_gap_p95_ms=120
```

```text
[NGX_WARN] type=timeout_read_spike upstream=ingestor-2:8080 count_10s=37 retry_success=21 retry_fail=16 action=check_ingestor_latency_and_network
```

---

## 7) 임계치 초안 (운영 시작값)
- `status_5xx_rate >= 1%` 3분 지속 -> `WARN`
- `status_5xx_rate >= 3%` 1분 지속 -> `ERROR`
- `status_499_rate >= 3%` 5분 지속 -> `WARN`
- `timeout_read_count >= 30/10s` 3회 연속 -> `WARN`
- `retry_invoked_rate >= 5%` 5분 지속 -> `WARN`
- `retry_exhausted_rate >= 1%` 3분 지속 -> `ERROR`
- `retry_success_rate < 70%` 5분 지속 -> `WARN`
- 특정 upstream `share_pct >= 60%` 5분 지속 -> `WARN`
- 특정 upstream `share_pct = 0%` (전체 `rps > 100` 조건) 1분 지속 -> `ERROR`
- `downstream_gap_ms_p95 >= 500ms` 5분 지속 -> `WARN`
- `downstream_gap_ms_p95 >= 2000ms` 2분 지속 -> `ERROR`

메모:
- 위 값은 초기 기준이며, 운영 1주일 baseline 관측 후 조정한다.

---

## 8) 검증 시나리오 (구현 후)
1. 정상 부하:
   - 세 upstream 분배율이 33% 내외(편차 허용 범위 내)인지 확인
   - `status_5xx_rate`, `retry_exhausted_rate`가 0에 수렴하는지 확인
2. upstream 1대 중지:
   - `retry_invoked_rate` 단기 상승 후 회복
   - 중지된 upstream `share_pct` 0, 나머지 2대로 재분배되는지 확인
3. upstream 지연 유도:
   - `upstream_header_time_ms_p95`와 `upstream_response_time_ms_p95` 상승 확인
   - timeout 분류(`connect/send/read`) 중 `read` 중심으로 증가하는지 확인
4. 느린 클라이언트 유도:
   - `downstream_gap_ms_p95`, `connections_writing` 상승
   - upstream 지연은 상대적으로 안정적인지 확인

---

## 9) 운영 체크리스트
- upstream 분배율이 장시간 한쪽으로 치우치지 않는가?
- timeout이 특정 upstream/특정 시간대에 집중되는가?
- timeout 발생 시 retry 성공률이 충분히 높은가?
- retry_exhausted가 증가하는가?
- `request_time` 대비 `upstream_response_time` 격차가 커지는가?
- Nginx 메모리/writing 연결 수가 동시에 상승하는가?
- `status_499`와 `status_5xx`가 분리 해석되고 있는가?
- request 단위 추적 시 `request_id`로 ingress->upstream 상관분석이 가능한가?
