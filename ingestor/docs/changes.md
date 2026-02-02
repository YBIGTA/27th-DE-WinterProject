# Ingestor 코드 변경 내역

> 커밋: `edcf168`, `5825a29`

---

## 1. Dockerfile (신규)

**파일**: `ingestor/Dockerfile`

**왜 만들었나?**
- Ingestor를 Docker 컨테이너로 실행하기 위해

**주요 내용**:
```dockerfile
# 1단계: 빌드
FROM gradle:8.5-jdk17 AS build
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle build -x test --no-daemon

# 2단계: 실행
FROM eclipse-temurin:17-jre
RUN apt-get install -y curl          # healthcheck용
RUN useradd -r appuser               # 보안: 비루트 사용자
COPY --from=build app.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

**기능상 바뀐 점**:
- 멀티스테이지 빌드로 이미지 크기 최소화
- 비루트 사용자(`appuser`)로 실행 → 보안 강화
- curl 설치 → healthcheck에서 사용

---

## 2. docker-compose.yml (신규)

**파일**: `ingestor/docker-compose.yml`

**왜 만들었나?**
- Ingestor 3개 + Nginx LB를 한 번에 띄우기 위해

**주요 내용**:
```yaml
services:
  ingestor-1:    # 포트 8081
  ingestor-2:    # 포트 8082
  ingestor-3:    # 포트 8083
  nginx-lb:      # 포트 8080 (로드밸런서)
  kafka-healthcheck:  # Kafka 대기용
```

**기능상 바뀐 점**:
| 항목 | 내용 |
|------|------|
| 클러스터링 | 3개 인스턴스 동시 실행 |
| 로드밸런싱 | Nginx가 8080에서 3개로 분산 |
| 헬스체크 | 10초마다 `/health` 확인, 3회 실패 시 재시작 |
| 네트워크 | `kafka_kafka-network`에 연결 (Kafka와 통신) |
| 환경변수 | `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:29092` |

---

## 3. nginx.conf (신규)

**파일**: `ingestor/nginx.conf`

**왜 만들었나?**
- 3개 Ingestor 인스턴스에 요청을 분산하기 위해

**주요 내용**:
```nginx
upstream ingestors {
    least_conn;                    # 최소 연결 알고리즘
    server ingestor-1:8080;
    server ingestor-2:8080;
    server ingestor-3:8080;
}
```

**기능상 바뀐 점**:
| 설정 | 값 | 이유 |
|------|-----|------|
| `least_conn` | - | 현재 연결 적은 서버로 분산 (균등 분배) |
| `worker_connections` | 4096 | 동시 연결 수 증가 |
| `proxy_buffering` | off | 버퍼링 없이 바로 전달 (지연 최소화) |
| `proxy_connect_timeout` | 5s | 업스트림 연결 타임아웃 |

---

## 4. IngestionController.java (수정)

**파일**: `ingestor/src/main/java/com/ingestion/controller/IngestionController.java`

**어디가 바뀌었나?**

### 4.1 `/health` 엔드포인트 추가
```java
@GetMapping("/health")
public Mono<ResponseEntity<String>> health() {
    return Mono.just(ResponseEntity.ok("OK"));
}
```
**이유**: Docker healthcheck에서 호출하기 위해

### 4.2 `FAIL_NON_SERIALIZED` 처리 추가
```java
case FAIL_NON_SERIALIZED:
    log.warn("[CONCURRENCY] Event emission failed...");
    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build());
```
**이유**: 동시성 문제 발생 시 503 반환 (클라이언트가 재시도하도록)

---

## 5. IngestionService.java (수정)

**파일**: `ingestor/src/main/java/com/ingestion/service/IngestionService.java`

**어디가 바뀌었나?**

### 5.1 EmitFailureHandler 추가
```java
private final Sinks.EmitFailureHandler emitFailureHandler = (signalType, emitResult) -> {
    if (emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
        return true;   // 재시도
    }
    return false;      // 다른 에러는 재시도 안 함
};
```
**이유**: 고부하 시 여러 스레드가 동시에 emit하면 `FAIL_NON_SERIALIZED` 발생 → 자동 재시도

### 5.2 ingest() 메서드 수정
```java
public Sinks.EmitResult ingest(TaxiEvent event) {
    Sinks.EmitResult result = sink.tryEmitNext(event);

    if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
        // 동시성 문제 → emitNext로 재시도
        sink.emitNext(event, emitFailureHandler);
        return Sinks.EmitResult.OK;
    }
    // ...
}
```
**이유**: `tryEmitNext`가 동시성 문제로 실패하면, `emitNext`로 재시도

---

## 6. application.yml (수정)

**파일**: `ingestor/src/main/resources/application.yml`

**어디가 바뀌었나?**

### Before
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
app:
  kafka:
    topic: taxi-event-data
```

### After
```yaml
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
app:
  kafka:
    topic: ${APP_KAFKA_TOPIC:taxi-event-data}
```

**이유**: 환경변수로 설정 가능하게 변경
- 로컬: 기본값 `localhost:9092` 사용
- Docker: 환경변수 `kafka:29092` 주입

---

## 7. docker-compose.ingestor.yml (신규)

**파일**: `ingestor/docker-compose.ingestor.yml`

**왜 만들었나?**
- 인스턴스를 **개별적으로** 띄울 수 있게 하기 위해
- 분산 환경에서 각 서버에서 따로 실행할 때 사용

**사용법**:
```bash
INGESTOR_ID=1 INGESTOR_PORT=8081 docker compose -f docker-compose.ingestor.yml up -d
INGESTOR_ID=2 INGESTOR_PORT=8082 docker compose -f docker-compose.ingestor.yml up -d
```

**기능상 바뀐 점**:
- 환경변수로 ID, 포트, Kafka 주소 지정 가능
- 한 파일로 여러 인스턴스 관리

---

## 8. docker-compose.nginx.yml (신규)

**파일**: `ingestor/docker-compose.nginx.yml`

**왜 만들었나?**
- Nginx LB만 **따로** 띄울 수 있게 하기 위해
- Ingestor들이 다른 서버에 있을 때 사용

**사용법**:
```bash
# 먼저 nginx-standalone.conf에서 IP 주소 수정 후
docker compose -f docker-compose.nginx.yml up -d
```

---

## 9. nginx-standalone.conf (신규)

**파일**: `ingestor/nginx-standalone.conf`

**왜 만들었나?**
- 분산 환경에서 **IP 주소로 직접** Ingestor 지정하기 위해

**nginx.conf와 차이점**:
```nginx
# nginx.conf (Docker 내부용)
server ingestor-1:8080;   # 컨테이너 이름 사용

# nginx-standalone.conf (분산 환경용)
server 192.168.0.101:8081;   # IP 주소 사용
```

**사용 시나리오**:
- Ingestor가 서로 다른 물리 서버에 있을 때
- Tailscale 등으로 연결된 환경에서

---

## 요약

| 파일 | 변경 유형 | 핵심 목적 |
|------|----------|----------|
| Dockerfile | 신규 | Docker 이미지 빌드 |
| docker-compose.yml | 신규 | 3개 클러스터 + LB 한 번에 실행 |
| nginx.conf | 신규 | least_conn 로드밸런싱 |
| IngestionController | 수정 | `/health` 추가, 503 처리 |
| IngestionService | 수정 | 동시성 문제 재시도 로직 |
| application.yml | 수정 | 환경변수 지원 |
| docker-compose.ingestor.yml | 신규 | 개별 인스턴스 실행용 |
| docker-compose.nginx.yml | 신규 | LB만 따로 실행용 |
| nginx-standalone.conf | 신규 | IP 주소 직접 지정용 |
