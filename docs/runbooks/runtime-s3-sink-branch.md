# Runtime Runbook - Optional S3 Sink Branch

이 문서는 선택 기능인 `kafka -> kafka connect -> S3` 분기를 구성할 때 사용합니다.
기본 파이프라인과는 별도 옵션입니다.

## 0. 전제

1. Kafka 클러스터가 이미 정상 동작 중이어야 합니다.
2. AWS 계정/IAM 권한이 준비되어 있어야 합니다.
3. Kafka Connect worker는 이 저장소에서 직접 제공하지 않습니다. (별도 운영)

## 1. Terraform으로 S3/IAM 준비

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject/infra/terraform
terraform init
terraform plan
terraform apply
```

## 2. Connector 설정 파일 준비

```bash
cd /home/sleepylee/Desktop/0proj/27th-DE-WinterProject/infra/connectors
cp s3-sink-config.template.json s3-sink-config.json
```

`s3-sink-config.json`에서 아래를 실제 값으로 수정:

1. S3 bucket 이름
2. AWS Access Key / Secret Key
3. topic / flush / format 관련 옵션

## 3. Kafka Connect worker 준비

Kafka Connect worker를 별도 환경에서 기동합니다.
예시 endpoint:

- `http://<connect-host>:8083`

## 4. Connector 등록

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @s3-sink-config.json \
  http://localhost:8083/connectors
```

## 5. 검증

등록 확인:

```bash
curl -s http://localhost:8083/connectors | python3 -m json.tool
curl -s http://localhost:8083/connectors/<connector-name>/status | python3 -m json.tool
```

S3 확인:

1. 지정한 prefix에 파일 생성 여부
2. 레코드 수/포맷(JSON/Parquet 등) 정합성

## 6. 장애 시 1차 점검

1. Kafka Connect 로그에서 auth/permission 오류 확인
2. IAM policy에 bucket write 권한(`PutObject`, `ListBucket`) 포함 여부 확인
3. Kafka bootstrap 주소가 실제 reachable 주소인지 확인
