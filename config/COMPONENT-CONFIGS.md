# Component Config Spec (Phase 2)

Status: `CURRENT` (core wiring completed)

## 목적
- `.env`는 네트워크 경로(IP/PORT)만 관리
- 컴포넌트 YAML은 기능/성능 튜닝 관리
- 실행은 `ops/compose/*` + `--env-file config/.env` 기준

## Component Config Matrix
| Component | Config Path | Format | Runtime Load |
|---|---|---|---|
| Ingestor | `services/ingestor/config/default.yaml` | YAML | Spring `spring.config.import` |
| Generator | `services/generator/config/default.yaml` | YAML | `generate.cpp` YAML parser |
| Flink job | `services/flink-job/config/default.yaml` | YAML | `FLINK_CONFIG_PATH` + SnakeYAML |
| Kafka | `infra/kafka/config/default.yaml` | YAML(flat map) | `infra/kafka/entrypoint.sh` sources YAML -> env export |
| ClickHouse | `infra/clickhouse/config/default.yaml` | YAML | native config at `/etc/clickhouse-server/config.d/` |
| Nginx (SM) | `infra/nginx/nginx.single-machine.conf` | conf | static mount to `/etc/nginx/nginx.conf` |
| Nginx (DM) | `infra/nginx/templates/nginx.distributed.conf.template` | template | `envsubst` (`.env` IP/PORT only) |
| S3 Connector | `infra/connectors/s3-sink-config.template.json` | JSON | Kafka Connect REST payload |

## Compose 연결 원칙
1. `config/.env`를 먼저 준비 (`.env.single-machine` 또는 `.env.distributed` 복사)
2. compose는 `--env-file config/.env`로 실행
3. 앱 컨피그는 각 서비스에서 마운트/로딩

## 제약
- Kafka YAML은 flat key 구조 유지 권장 (entrypoint.sh awk 파서)
- Nginx 튜닝 값은 conf 파일에 직접 기술 (env 아님)
- Generator는 compose 서비스가 아니라 native 실행(`services/generator/README.md`)
