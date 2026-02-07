# Component Config Spec (Demolish Ops)

Status: `CURRENT`

## 목적
- `.env`는 네트워크(IP/PORT)만 관리
- 배포 서비스의 non-network 설정은 컴포넌트 compose에 하드코딩
- 실행은 각 컴포넌트 compose 파일 직접 실행 기준

## Component Runtime Matrix
| Component | Runtime Owner | Runtime Values Source |
|---|---|---|
| Kafka | `infra/kafka/docker-compose.*.yml` | compose `environment` (hardcoded tuning) |
| ClickHouse | `infra/clickhouse/docker-compose.*.yml` | compose `environment` + schema mount |
| Nginx (SM) | `infra/nginx/docker-compose.yml` | static `infra/nginx/nginx.single-machine.conf` |
| Nginx (DM) | `infra/nginx/docker-compose.distributed.yml` | template + `.env` IP/PORT envsubst |
| Ingestor | `services/ingestor/docker-compose.*.yml` | compose `environment` (`APP_*`, `SPRING_*`) |
| Flink | `infra/flink/docker-compose.*.yml` | compose `environment` (`FLINK_*`) |
| Generator | native run (`services/generator`) | `services/generator/config/default.yaml` |

## Compose 연결 원칙
1. `config/.env` 준비 (`.env.single-machine` 또는 `.env.distributed` 복사)
2. 필요한 컴포넌트 compose만 직접 실행
3. 항상 `--env-file config/.env` 명시

## 제약
- `config/.env`에는 `*_IP`, `*_PORT`만 허용
- 배포 서비스에서 `*/config/default.yaml` 마운트 금지
- Generator는 compose 서비스가 아니라 native 실행
