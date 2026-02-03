# AWS S3 Data Lake Infrastructure (Terraform)

이 디렉토리는 NYC Taxi 데이터를 적재하기 위한 **AWS S3 버킷**과, Kafka Connect가 해당 버킷에 접근할 수 있도록 권한을 관리하는 **IAM User**를 생성하는 Terraform 코드입니다.

## 사전 요구 사항 (Prerequisites)

이 코드를 실행하기 위해서는 아래 도구들이 설치되어 있어야 합니다.

### 1. 도구 설치 (Environment)
* **Terraform**: v1.0.0 이상
    * Mac: `brew tap hashicorp/tap && brew install hashicorp/tap/terraform`
    * Windows: [Terraform 다운로드 페이지](https://developer.hashicorp.com/terraform/install) 참조
* **AWS 계정**: S3 및 IAM 생성 권한이 있는 관리자 계정 필요

### 2. AWS 자격 증명 (Credentials)
Terraform이 AWS 리소스를 생성하려면 액세스 키가 필요합니다. 보안을 위해 코드가 아닌 별도의 변수 파일로 관리합니다.

1. `infra/terraform/` 폴더 내에 `terraform.tfvars` 파일을 생성합니다.
2. 아래 내용을 복사하여 본인의 AWS Admin Key를 입력합니다.

```hcl
# terraform.tfvars (이 파일은 Git에 업로드하지 않습니다)

aws_access_key = "AKIA..."  <-- 본인의 Access Key 입력
aws_secret_key = "......"   <-- 본인의 Secret Key 입력
```

## 실행 방법 (Usage)

터미널에서 `infra/terraform` 디렉토리로 이동한 뒤 순서대로 실행하세요.

### 1. 초기화 (Init)
Terraform 플러그인과 백엔드를 초기화합니다.

```
terraform init
```

### 2. 계획 확인 (Plan)
어떤 리소스가 생성될지 미리 확인합니다.

```
terraform plan
```

### 3. 인프라 생성 (Apply)
실제로 AWS 리소스를 생성합니다. 중간에 `yes`를 입력해야 합니다.

```
terraform apply
```

## 결과 확인 (Outputs)
실행이 완료되면 터미널에 Kafka Connect 설정에 필요한 접속 키(Access Key / Secret Key)가 출력됩니다.
만약 Secret Key가 가려져서(`<sensitive>`) 안 보인다면 아래 명령어로 확인하세요.

```
# 전체 Output을 JSON 형태로 확인
terraform output -json
```
* connector_access_key: S3 Sink Connector 설정에 사용
* connector_secret_key: S3 Sink Connector 설정에 사용

## 리소스 상세 (Resources)
* S3 Bucket: `nyc-taxi-raw-2026-ybigta-de` (Raw 데이터 저장소)
* IAM User: `nyc-taxi-s3-sink-connector` (Kafka Connect 전용 사용자)
* IAM Policy: 해당 버킷에 대한 `PutObject`, `GetObject` 권한 부여