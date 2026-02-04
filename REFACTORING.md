# Refactoring Plan (Ops-Centric Layout)

이 문서는 구조 개편(Phase 1)과 설정 통합(Phase 2)의 상태를 요약한다.
실행 방법의 최신 기준은 `config/README.md`다.

## Goals
1. 애플리케이션 코드(`services`)와 운영 진입점(`ops/compose`) 분리
2. 인프라 런타임 설정을 `infra/*/config/default.yaml`로 일관화
3. `config/.env`를 네트워크(IP/PORT) 전용으로 제한

## Phase Status
### Phase 1: Structure Refactor
Status: `DONE`

완료:
- 디렉토리 재배치 (`services/`, `infra/`, `ops/compose/...`)
- compose entrypoint를 `ops/compose/single-machine`, `ops/compose/distributed`로 통합
- 경로/문서 업데이트

### Phase 2: Config Consolidation
Status: `DONE (implementation)`

완료:
- 컴포넌트별 YAML 기본 설정 추가
- Kafka/ClickHouse/Nginx compose startup에서 YAML 파싱 후 runtime env 주입
- Ingestor(Spring), Generator(C++), Flink(SnakeYAML) 설정 로딩 연결
- distributed instance registry(IP 기반) 반영

남은 운영 작업:
- 환경별 실기동 smoke test 기록 정리
- 경고 정리(`version:` obsolete) 여부 결정

## Target Layout
```text
.
├── services
│   ├── ingestor
│   ├── generator
│   └── flink-job
├── preprocess
├── infra
│   ├── kafka
│   ├── clickhouse
│   ├── flink
│   ├── spark
│   ├── nginx
│   └── connectors
├── ops
│   └── compose
│       ├── single-machine
│       └── distributed
└── config
    ├── .env.single-machine
    ├── .env.distributed
    └── README.md
```

## Compose 실행 규약
프로젝트 루트 기준:
```bash
docker compose -f ops/compose/<mode>/docker-compose.yml --env-file config/.env up -d <service...>
```

이 규약을 쓰는 이유:
- distributed `${VAR}` 치환값을 compose 파싱 시점에 보장하기 위해
- 실행 위치(cwd) 차이에 따른 상대경로 혼란을 줄이기 위해

## Validation Checklist
1. `config/README.md`의 실행 커맨드대로 기동 가능
2. `docker compose ... config`가 single/distributed 모두 통과
3. Ingestor/Flink topic 값이 일치
4. Flink ClickHouse target이 `infra/clickhouse/schema.sql`와 일치
