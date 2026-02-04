# Flink Processor

이 디렉토리는 NYC 택시 이벤트를 실시간으로 정제(Enrichment), 분석 및 적재하기 위한 **Apache Flink 스트림 프로세싱 엔진**의 구현체와 가이드를 포함합니다.

Kafka로부터 유입되는 원시 데이터를 정제하여 OLAP 시스템(ClickHouse)에 적재하고, 3분 단위 수요 지표를 산출하여 **실시간 로그(Stdout)로 출력**함으로써 데이터 흐름을 모니터링하는 역할을 수행합니다.

## 📂 디렉토리 구조 (Directory Structure)

```text
.
├── infra/
│   └── flink/
│       ├── Explanation.md           # Flink 프로세서 기술 명세서 (V13.1)
│       └── README.md                # 인프라 가이드
├── ops/
│   └── compose/
│       ├── single-machine/
│       │   └── flink.yml            # 로컬 실행용 Flink compose
│       └── distributed/
│           └── flink.yml            # 분산 실행용 Flink compose
├── services/
│   └── flink-job/                   # Flink 애플리케이션 모듈
│       ├── src/main/java/com/example/
│       │   ├── TaxiRealtimeJob.java    # 메인 파이프라인 로직
│       │   ├── TaxiEvent.java          # 데이터 모델 (DTO)
│       │   └── SpatialJoinFunction.java # 위경도-ZoneID 매핑 함수
│       ├── src/main/resources/         # 매핑용 CSV 데이터
│       └── pom.xml                     # Maven 빌드 및 의존성 설정
├── services/ingestor/               # 데이터 생성 및 Kafka 인입 모듈
└── README.md                        # 프로젝트 메인 가이드
```

## 🛠 의존성 (Dependencies)

`services/flink-job/pom.xml`에 정의된 핵심 의존성 라이브러리 목록입니다.

* **Runtime**: Java 11, Apache Flink 1.18
* **Libraries**:
    * `flink-connector-kafka`
    * `flink-connector-jdbc`
    * `jackson-databind` / `jackson-core`
    * `flink-clients`

## 🚀 실행 가이드 (Quick Start)

### 1. 환경 준비 (Prerequisites)
* **Maven 설치**: [Apache Maven Download](https://maven.apache.org/download.cgi)에서 설치 후 `mvn -version`으로 확인하세요.
* **인프라 가동**:
  ```bash
  docker compose -f ops/compose/single-machine/flink.yml up -d
  ```

### 2. 참조 데이터(CSV) 주입 (최초 1회)
`SpatialJoinFunction`이 참조할 위경도 데이터를 TaskManager 컨테이너로 복사합니다.

```bash
# TaskManager 내부에 리소스 폴더 생성
docker exec flink-taskmanager mkdir -p /opt/flink/resources

# 로컬의 CSV 파일을 컨테이너 내부로 복사
docker cp services/flink-job/src/main/resources/taxi_zone_median_coords.csv flink-taskmanager:/opt/flink/resources/
```

### 3. 빌드 및 Job 실행

```bash
# 1) Maven 빌드
cd services/flink-job
mvn clean package

# 2) JAR 파일 복사
docker cp target/flink-kafka-print-0.1.0.jar flink-jobmanager:/opt/flink/usrlib/

# 3) Flink Job 실행
docker exec flink-jobmanager flink run -c com.example.TaxiRealtimeJob /opt/flink/usrlib/flink-kafka-print-0.1.0.jar
```

### 4. 결과 확인 및 테스트

**로그 모니터링:**
```bash
docker logs -f flink-taskmanager
```

**테스트 데이터 전송 (Kafka Producer):**
```bash
# 새로운 터미널 창에서 실행
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic taxi_raw_events

# 접속 후 아래 JSON 입력 예시:
{"trip_id": 110, "event": "PICKUP", "ts": "2026-02-02T15:00:05Z", "lat": 40.7244, "lon": -73.9734, "total_amount": 15.5}
```

## 🏗 핵심 기능 (Core Features)

* **Dual-Track Data Flow (이중 경로 처리)**:
    * **Track 1 (Raw Storage)**: Spatial Join으로 정제된 개별 이벤트를 ClickHouse(JDBC)에 즉시 적재하여 실시간 조회를 지원합니다.
    * **Track 2 (Metric Aggregation)**: 3분 단위 Tumbling Window를 통해 구역별 수요 지표를 산출하여 Stdout으로 스트리밍합니다.
* **Spatial Join Enrichment**: 위경도 데이터를 기반으로 구역 ID를 매핑하여 원본 데이터를 실시간으로 보강합니다.
* **Reliability & Accuracy**: 역직렬화 예외 처리 및 Event-time 기반 워터마크(5s 지연 허용)를 적용하여 데이터 정합성을 보장합니다.
