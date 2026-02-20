# Nginx Logging Guide

## 1) 목적
이 문서는 Nginx LB의 라우팅/장애대응 상태를 운영 관점에서 추적하기 위한 로깅 기준을 정의한다.

핵심 목표:
- upstream(ingestor-1/2/3) 분배 비율을 주기적으로 확인
- timeout 발생 시 원인(어느 단계에서 멈췄는지)과 결과(재시도 성공/실패)를 확인
- proxy buffer 관련 병목 신호를 간접 지표로 감지
- `proxy_next_upstream` 재시도 동작이 실제로 어떻게 수행되는지 확인

---

## 2) 기본 원칙
- `INFO`: 주기 요약 로그(기본 10초)
- `WARN`: 특정 upstream 편중, timeout 급증, retry 급증
- `ERROR`: 모든 upstream 실패로 최종 에러 반환 급증
- `DEBUG`: 샘플링 상세(특정 request 추적 시 한시적으로)

권장:
- 요청 원문(body) 로그 금지
- “접근 로그 집계 + 에러 로그 패턴 + 임계치 경보” 조합 사용

---

## 3) 로깅 항목 (요청안 기준)

### 3.1 주기별 upstream 분배량
목적:
- `least_conn` 분산이 실제로 어느 upstream으로 얼마나 갔는지 확인
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

### 3.2 timeout 원인 + 결과 로깅
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

### 3.3 buffer 상태 로깅 (현실적 범위)
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

### 3.4 upstream 재시도 로직 상태
목적:
- `proxy_next_upstream` 경로가 얼마나 자주 동작하는지 파악
- 재시도 후 성공/실패 분포 확인

권장 집계:
- `retry_invoked_count` (다중 `upstream_status` 라인 수 기반)
- `retry_success_count` (초기 실패 후 최종 2xx)
- `retry_exhausted_count` (재시도 후도 최종 실패)
- `peer_marked_failed_events` (`max_fails/fail_timeout` 관련 error 로그 패턴 기반)

메모:
- 요청 단위 다중 시도를 보려면 `upstream_status`/`upstream_addr`의 콤마 포함 라인을 분석한다.

---

## 4) 추가 권장 항목
- access log 확장(가능하면):
  - `upstream_connect_time`
  - `upstream_header_time`
  - `request_length`
  - `bytes_sent`
- error log 레벨/보존 정책 명시:
  - timeout 원인 분석이 가능하도록 rotation + 보존 기간 설정
- 모니터링 연계:
  - Loki/Promtail로 `access.log`, `error.log` 수집
  - 10초 윈도우 집계 패널(분배율, timeout, retry, 최종 상태코드) 구성

---

## 5) 권장 출력 예시
```text
[NGX_METRICS] ts=... win=10s req_total=12450 up1=4120(33.1%) up2=4189(33.6%) up3=4141(33.3%) retry_invoked=182 retry_success=169 retry_exhausted=13 timeout_connect=2 timeout_send=4 timeout_read=21 status_2xx=12390 status_4xx=20 status_5xx=40
```

```text
[NGX_WARN] type=timeout_read_spike upstream=ingestor-2:8080 count_10s=37 retry_success=21 retry_fail=16 action=check_ingestor_latency_and_network
```

---

## 6) 운영 체크리스트
- upstream 분배율이 장시간 한쪽으로 치우치지 않는가?
- timeout이 특정 upstream/특정 시간대에 집중되는가?
- timeout 발생 시 retry 성공률이 충분히 높은가?
- retry_exhausted가 증가하는가?
- `request_time` 대비 `upstream_response_time` 격차가 커지는가?
- Nginx 메모리/writing 연결 수가 동시에 상승하는가?
