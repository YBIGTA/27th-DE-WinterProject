---
status: DONE
created: 2026-02-04
last_updated: 2026-02-04
purpose: phase-2-config-consolidation-history
---

# Config Upgrade Plan (Completed)

이 문서는 Phase 2 설정 개편의 완료 기록입니다.
현재 운영 기준은 `config/README.md`를 따른다.

## 완료 목표
1. `.env`를 네트워크(IP/PORT) 전용으로 정리
2. non-network 설정을 컴포넌트 YAML로 이동
3. single-machine / distributed compose 모두 새 설정 모델과 연결
4. distributed 인스턴스 레지스트리(IP 기반) 적용

## 결과 요약
- `config/.env.single-machine`, `config/.env.distributed` 정리 완료
- `services/*/config/default.yaml`, `infra/*/config/default.yaml` 도입 완료
- Ingestor/Spring, Generator/C++, Flink/SnakeYAML 연동 완료
- Kafka/ClickHouse/Nginx compose startup에서 YAML 파싱 후 env export 완료
- distributed compose에서 `${KAFKA_*_IP}` 기반 파생값 사용 완료

## 생성/변경된 핵심 파일
- `config/.env.single-machine`
- `config/.env.distributed`
- `config/COMPONENT-CONFIGS.md`
- `services/ingestor/config/default.yaml`
- `services/generator/config/default.yaml`
- `services/flink-job/config/default.yaml`
- `infra/kafka/config/default.yaml`
- `infra/clickhouse/config/default.yaml`
- `infra/nginx/config/default.yaml`
- `ops/compose/single-machine/docker-compose.*.yml`
- `ops/compose/distributed/docker-compose.*.yml`

## 운영 시 주의 (확정)
1. compose 실행 시 `--env-file config/.env`를 항상 명시
2. `config/.env`에는 `*_IP`, `*_PORT` 외 값 추가 금지
3. topic/table 정합성은 app YAML과 schema 기준으로 관리

## 참조 문서
- 실행 기준: `config/README.md`
- 상세 구조: `config/EXPLANATION.md`
- 컴포넌트 매핑: `config/COMPONENT-CONFIGS.md`
