# Runtime Runbook Index (Scenario-based)

이 문서는 "통합 runbook" 대신, 상황별 runbook으로 바로 이동하기 위한 인덱스입니다.
각 runbook은 `처음부터(신규 머신/초기 상태)` 실행할 수 있게 독립 절차를 포함합니다.

## 1. 어떤 runbook을 써야 하는가

| 상황 | 사용할 문서 | 설명 |
|---|---|---|
| 로컬 1대에서 파이프라인 전체 실행 | `docs/runbooks/runtime-single-machine-from-scratch.md` | Core + Observability 전체 기동 |
| 실제 멀티 머신 분산 배포 | `docs/runbooks/runtime-distributed-from-scratch.md` | 머신 역할 분리 + Core/Observability 전체 기동 |
| 분산 설정을 1대에서 사전 검증 | `docs/runbooks/runtime-distributed-one-host-fallback-from-scratch.md` | distributed compose를 one-host로 재현 |
| 단일 머신 관측 스택만 단독/복구 기동 | `docs/runbooks/runtime-observability-single-machine.md` | Loki/Prometheus/Grafana 상세 기동/검증 |
| 분산 환경 관측 스택만 단독/복구 기동 | `docs/runbooks/runtime-observability-distributed.md` | distributed 기준 상세 기동/검증 |
| 단일 머신 정지/초기화 | `docs/runbooks/runtime-stop-reset-single-machine.md` | 안전 정지, 전체 정리, ClickHouse 부분 초기화 |
| 분산 환경 정지/초기화 | `docs/runbooks/runtime-stop-reset-distributed.md` | 머신별 down 순서와 주의사항 |
| S3 Sink 분기(옵션) | `docs/runbooks/runtime-s3-sink-branch.md` | Terraform + Kafka Connect 연동 |

## 2. 공통 원칙 (모든 runbook 공통)

1. 모든 명령은 프로젝트 루트(`/home/sleepylee/Desktop/0proj/27th-DE-WinterProject`)에서 실행합니다.
2. `docker compose`는 항상 `--env-file config/.env`를 명시합니다.
3. `config/.env`는 `IP/PORT`만 유지합니다. (topic/tuning 값은 compose가 source of truth)
4. `config/.env`는 git에 커밋하지 않습니다.

## 3. 실행 후 검증 문서

런타임 기동이 끝나면 아래 문서로 E2E 검증을 진행하세요.

- `docs/runbooks/validation.md`
