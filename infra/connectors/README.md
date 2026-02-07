# S3 Sink Connector Configuration

이 디렉토리는 Kafka Connect에 **AWS S3 Sink Connector**를 등록하고 관리하기 위한 설정 파일과 가이드를 포함합니다.

Kafka로 유입된 데이터를 **JSON 포맷**으로 변환하여 Terraform으로 생성된 S3 버킷에 적재하는 역할을 수행합니다.

## 파일 구조 (File Structure)

| 파일명                         | 설명                                                         |
| :----------------------------- | :----------------------------------------------------------- |
| `s3-sink-config.template.json` | 커넥터 설정 템플릿 파일 (민감 정보 제외됨)                   |
| `s3-sink-config.json`          | **(Git 제외)** 실제 배포에 사용되는 설정 파일. API Key 포함. |
| `.gitignore`                   | 민감한 설정 파일(`*.json`)을 Git 추적에서 제외               |

---

## Kafka Connect 준비 (Prerequisites)

> **⚠️ 주의 (Notice):**
> 이 레포는 Kafka Connect 워커를 직접 포함하지 않습니다.
> 운영 환경(Production)에서는 **Kafka Cluster 구성, 보안 설정(SSL/SASL), 데이터 볼륨 관리** 등이 추가로 고려되어야 합니다.

커넥터를 등록하기 전에, **Kafka Connect 워커**가 정상적으로 실행 중이어야 합니다.

### 1. 워커 실행
Kafka Connect 워커는 별도 환경(로컬/클러스터)에서 실행합니다.
로컬 테스트용 docker-compose를 별도로 사용하는 경우, 워커 컨테이너 이름이 `connect`인지 확인하세요.

### 2. 구동 상태 확인
Kafka Connect가 실행될 때 S3 플러그인을 다운로드하므로, 약 1~2분의 초기화 시간이 필요합니다. 로그를 확인하여 준비가 끝났는지 체크하세요.

```bash
docker logs -f connect
```
확인: 로그 마지막 쪽에 `Finished starting connectors and tasks (org.apache.kafka.connect.runtime.distributed.DistributedHerder)`와 비슷한 문구가 보이면 커넥터 등록 준비가 완료된 것입니다.

## 설정 준비 (Configuration Setup)

보안을 위해 실제 Access Key가 포함된 파일은 Git에 업로드하지 않습니다. 템플릿을 기반으로 로컬 설정 파일을 생성해야 합니다.

### 1. 설정 파일 생성
```bash
cp s3-sink-config.template.json s3-sink-config.json
```

### 2. 필수 값 입력
* s3.bucket.name: 생성된 S3 버킷 이름
* aws.access.key.id: Connector IAM User의 Access Key
* aws.secret.access.key: Connector IAM User의 Secret Key

### 3. 주요 설정 설명
이 파이프라인은 Schema-less JSON을 처리하도록 되어 있습니다.

| 파라미터                         | 값 (권장)          | 설명                                                                  |
| :------------------------------- | :----------------- | :-------------------------------------------------------------------- |
| `topics`                         | `taxi-event-data`  | 구독할 Kafka 토픽 이름                                                |
| `flush.size`                     | `3`                | 메모리에 이 숫자만큼 데이터가 모이면 S3에 업로드 (운영 시 1000+ 권장) |
| `value.converter`                | `...JsonConverter` | 데이터를 JSON으로 처리                                                |
| `value.converter.schemas.enable` | `false`            | 스키마가 없는 평범한 JSON 데이터를 처리하기 위한 필수 설정            |

## 배포 및 관리 (Deployment & Management)
Docker 환경이 준비되고 설정 파일이 생성되었다면, 아래 명령어로 커넥터를 관리합니다.

### 커넥터 등록 (Create/Deploy)
```bash
curl -X POST -H "Content-Type: application/json" \
     --data @s3-sink-config.json \
     http://localhost:8083/connectors
```

### 상태 확인 (Check Status)
RUNNING 상태여야 정상 작동 중입니다.

```bash
curl http://localhost:8083/connectors/s3-sink-connector/status | python3 -m json.tool
```

### 설정 업데이트 (Update Config)
설정 파일 수정 후 재배포 시 사용합니다.

```bash
curl -X PUT -H "Content-Type: application/json" \
     --data @s3-sink-config.json \
     http://localhost:8083/connectors/s3-sink-connector/config
```

### 커넥터 삭제 (Delete)
초기화가 필요할 때 사용합니다.

```bash
curl -X DELETE http://localhost:8083/connectors/s3-sink-connector
```
