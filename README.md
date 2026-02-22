# 27th-DE-WinterProject
Reliable, scalable, fault-tolerant data pipeline simulation project.

This branch supports two deployment implementations, both without Kubernetes:
1. Single-machine
2. Distributed multi-machine

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
├── model
└── services
    ├── flink-job
    ├── generator
    └── ingestor
```

## Pipeline
Core path:
`generator -> nginx -> ingestor -> kafka -> flink -> clickhouse`

Prediction branch (current default):
`model (ONNX artifact) -> flink -> clickhouse.taxi_predictions`

Optional branches:
1. `kafka -> kafka connect s3 sink -> S3`
2. `prometheus + kafka-exporter + grafana`

## Documentation priority
1. Runtime runbook index (scenario split): `docs/runbooks/runtime.md`
2. Single-machine from scratch: `docs/runbooks/runtime-single-machine-from-scratch.md`
3. Distributed from scratch: `docs/runbooks/runtime-distributed-from-scratch.md`
4. Validation checklist: `docs/runbooks/validation.md`
5. Total architecture overview: `docs/system-architecture.md`
6. Config model + runtime ownership invariants: `config/EXPLANATION.md`
7. Refactor history: `docs/history/demolish-ops.md`, `docs/history/merge-issues-2026-02-04.md`

## Operating principles
1. `config/.env`는 IP/PORT만 관리한다.
2. non-network 값(topic, tuning, table 등)은 컴포넌트 compose 파일에 하드코딩한다.
3. 각 컴포넌트 compose 파일을 직접 실행한다 (root launcher 없음).
4. `docker compose` 실행 시 항상 `--env-file config/.env`를 명시한다.

## Quick start
Single-machine:
```bash
cp config/.env.single-machine config/.env

# Run each command in a separate terminal (foreground mode)
docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up --build
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up --build

cd services/generator
./build/generate
```

Distributed commands and machine-by-machine startup order are in `docs/runbooks/runtime-distributed-from-scratch.md`.

## Environment
1. flink-job: build with JDK 17+, target bytecode Java 11 (Flink 1.17.2 compatibility)
2. ingestor: JDK 17+ (Spring Boot 3.2)
3. generator: C++ toolchain (Conan + CMake)
4. Python workspace: `data/` with `uv` (`data/pyproject.toml`, `data/uv.lock`)

## Build notes

### flink-job (services/flink-job)
```bash
cd services/flink-job
mvn clean package
```

**Version Compatibility:**
| Component | Version | Notes |
|-----------|---------|-------|
| Flink runtime | 1.17.2 | Docker image: `flink:1.17.2-scala_2.12` |
| Target bytecode | Java 11 | Matches Flink 1.17.2 runtime |
| flink-connector-jdbc | 3.1.1-1.17 | Must match Flink version (not 1.18) |
| flink-connector-kafka | 1.17.2 | Must match Flink version |
| Lombok | 1.18.30+ | Required if building with JDK 21+ |

Common issues:
- `NoSuchFieldError: JCTree$JCImport` → Lombok version too old for your JDK. Upgrade Lombok.
- `ClassNotFoundException` at runtime → Connector version mismatch with Flink runtime.

### ingestor (services/ingestor)
```bash
cd services/ingestor
./gradlew build
# or via Docker
docker build -t ingestor .
```

Requires JDK 17+ (Spring Boot 3.2).
