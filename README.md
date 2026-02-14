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
1. 실행/운영 런북: `docs/runtime-runbook.md`
2. 이번 최적화 변경 이력/검증: `docs/optimization-changelog.md`
3. 설정 구조: `config/EXPLANATION.md`
4. 컴포넌트별 매핑: `config/COMPONENT-CONFIGS.md`

## 실행 원칙
1. `config/.env`는 IP/PORT만 관리한다.
2. non-network 값(topic, tuning, table 등)은 컴포넌트 compose 파일에 하드코딩한다.
3. 각 컴포넌트 compose 파일을 직접 실행한다 (root launcher 없음).

## Quick Start (single-machine)
```bash
# config/.env를 로컬에서 생성 (IP/PORT만 포함)
touch config/.env

docker compose -f infra/kafka/docker-compose.yml --env-file config/.env up
docker compose -f infra/clickhouse/docker-compose.yml --env-file config/.env up
docker compose -f services/ingestor/docker-compose.yml --env-file config/.env up
docker compose -f infra/nginx/docker-compose.yml --env-file config/.env up
docker compose -f infra/flink/docker-compose.yml --env-file config/.env up
```

## Environment
- flink-job: JDK 11 (Flink 1.17.2 requirement)
- ingestor: JDK 17 (Spring Boot 3.2 requirement)
- generator: C++ (Conan + CMake)
- Python: `uv`
- Local dev: JDK 17+ recommended

Python `uv` workspace files are in `data/` (`data/pyproject.toml`, `data/uv.lock`).

## Build Notes

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

**Common Issues:**
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
