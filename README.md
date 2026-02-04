# 27th-DE-WinterProject
Reliable, scalable, fault-tolerant data pipeline simulation project.

## Directory
```text
.
├── analysis
├── config
├── infra
├── ops
│   └── compose
│       ├── single-machine
│       └── distributed
├── data
├── preprocess
└── services
    ├── generator
    ├── ingestor
    └── flink-job
```

## 문서 우선순위
혼란 줄이기 위해 아래 순서로만 보면 됩니다.

1. 실행/운영: `config/README.md`
2. 설정 아키텍처 상세: `config/EXPLANATION.md`
3. Phase 진행 요약: `REFACTORING.md`
4. 컴포넌트별 설정 위치: `config/COMPONENT-CONFIGS.md`

## 실행 원칙 (Phase 2)
1. `config/.env`는 IP/PORT만 관리한다.
2. non-network 값(topic, tuning, table 등)은 각 YAML(`services/*/config/default.yaml`, `infra/*/config/default.yaml`)에서 관리한다.
3. Compose 실행은 항상 `--env-file config/.env`를 함께 사용한다.

## Quick Start
```bash
# 1) 모드 선택
cp config/.env.single-machine config/.env
# 또는
# cp config/.env.distributed config/.env

# 2) 원하는 서비스 기동 (root 기준)
docker compose -f ops/compose/single-machine/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3 clickhouse
```

## Environment
- JDK 21
- Python: `uv`
- C++ build: Conan + CMake
