---
component: loki-promtail-docker-compose
status: CURRENT
last_reviewed: 2026-02-09
core_files:
  - infra/loki/docker-compose.yml
  - infra/loki/loki-config.yml
  - infra/loki/promtail-config.yml
---

# loki-promtail-docker-compose

## Role
로컬/개발 환경에서 Loki + Promtail을 Docker Compose로 구동해 Docker 컨테이너 로그를 수집/조회한다.

## I/O Flow
```
[Docker Containers] --(json logs)--> [Promtail] --(HTTP push)--> [Loki] --(Query)--> [Grafana]
```

## Core behavior
1. Loki 컨테이너 실행 (grafana/loki:2.9.0)
2. Promtail 컨테이너 실행 (grafana/promtail:2.9.0)
3. Promtail이 `/var/lib/docker/containers/*/*-json.log` 수집
4. Loki는 `/tmp/loki`에 tsdb/인덱스를 저장
5. Grafana는 `http://loki:3100`로 로그 조회

## Design decisions
| Decision | Why | Trade-off |
|---|---|---|
| Loki TSDB + filesystem | 구성 단순화, 로컬 디스크 사용 | 디스크 사용량 증가 |
| Promtail 파일 tail 방식 | Docker 로그 수집 안정성 | Docker 디렉토리 마운트 필요 |
| kafka-network 공유 | Grafana와 내부 통신 보장 | 네트워크 선행 필요 |
| 높은 ingestion limit | 대량 로그 유입 처리 | 리소스 사용 증가 |

## Failure modes
| Failure | Detection | Response |
|---|---|---|
| Grafana에서 Loki 연결 실패 | Datasource Save & Test 실패 | Loki가 `kafka-network`에 있는지 확인 |
| 로그가 안 보임 | Grafana Explore 결과 없음 | Promtail 컨테이너 실행/로그 확인 |
| Docker 로그 접근 실패 | Promtail 로그에 permission 에러 | `/var/lib/docker` 마운트/권한 확인 |
| Rate limit(429) | Promtail 로그에 429 | `limits_config`의 ingestion 설정 상향 |
| config parse error | Promtail/Loki 시작 실패 | 설정 파일 YAML 스키마 확인 |

## Quick checks
```bash
# Loki 상태
curl http://localhost:3100/ready

# Promtail 상태
docker logs promtail | tail -20

# Loki 쿼리
curl -G 'http://localhost:3100/loki/api/v1/query' \
  --data-urlencode 'query={job="docker"}'
```
