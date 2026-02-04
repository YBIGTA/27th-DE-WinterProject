# 27th-DE-WinterProject
Reliable, scalable, fault-tolerant data pipeline simulation project.

## Directory
```text
.
├── config
├── data
│   ├── analysis
│   ├── preprocess
│   ├── pyproject.toml
│   └── uv.lock
├── infra
│   ├── clickhouse
│   ├── connectors
│   ├── flink
│   ├── kafka
│   ├── nginx
│   ├── spark
│   └── terraform
└── services
    ├── flink-job
    ├── generator
    └── ingestor
```

## 문서 우선순위
1. 실행/운영: `config/README.md`
2. 설정 구조: `config/EXPLANATION.md`
3. 컴포넌트별 매핑: `config/COMPONENT-CONFIGS.md`
4. 리팩토링 이력: `REFACTORING.md`

## 실행 원칙
1. `config/.env`는 IP/PORT만 관리한다.
2. non-network 값(topic, tuning, table 등)은 컴포넌트 compose 파일에 하드코딩한다.
3. 각 컴포넌트 compose 파일을 직접 실행한다 (root launcher 없음).

## Quick Start (single-machine)
```bash
cp config/.env.single-machine config/.env

docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up -d kafka-1 kafka-2 kafka-3 kafka-ui
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up -d clickhouse
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up -d ingestor-1 ingestor-2 ingestor-3
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up -d nginx-lb
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up -d flink-jobmanager flink
```

## Environment
- JDK 21
- Python: `uv`
- C++ build: Conan + CMake

Python `uv` workspace files are in `data/` (`data/pyproject.toml`, `data/uv.lock`).
