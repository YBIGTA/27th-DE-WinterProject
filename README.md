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
├── config
├── infra
├── ops
│   └── compose
│       ├── single-machine
│       └── distributed
├── data
│   ├── taxi_data
│   ├── taxi_data_preprocessed
│   └── taxi_zones
│       ├── shapeFiles
│       └── zoneInfo
├── services
│   ├── generator
│   ├── ingestor
│   ├── preprocess
│   └── flink-job
└── REFACTORING.md

```

### env
using jdk 21
using uv and conan
