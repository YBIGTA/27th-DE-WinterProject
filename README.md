# 27th-DE-WinterProject
Constructing Reliable, Scalable, Fault Tolerant Data Pipeline
Simulating Data pipeline equivalent to Uber


### project directory
```
.
├── analysis
│   └── outputs
│       ├── preprocessed
│       └── raw
├── data
│   ├── taxi_data
│   ├── taxi_data_preprocessed
│   └── taxi_zones
│       ├── shapeFiles
│       └── zoneInfo
├── generator
├── ingestor
└── preprocess

```

### env
using jdk 21
using uv and conan

---
## root의 `docker-compose.yml`에 대하여... (2026/02/02)
> **⚠️ 주의 (Notice):**
> 본 프로젝트의 `docker-compose.yml` 파일은 **로컬 개발 및 테스트 목적**으로 구성되었습니다.
> 운영 환경(Production)에서는 **Kafka Cluster 구성, 보안 설정(SSL/SASL), 데이터 볼륨 관리** 등이 추가로 고려되어야 합니다.