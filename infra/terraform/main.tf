# main.tf

provider "aws" {
  region = "ap-northeast-2"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

# 1. 데이터를 담을 S3 버킷
resource "aws_s3_bucket" "taxi_lake" {
  bucket = "nyc-taxi-raw-2026-ybigta-de"

  force_destroy = true
  
  tags = {
    Name        = "NYC Taxi Data Lake"
    Environment = "Dev"
  }
}

# 2. 버킷 보안 설정 (퍼블릭 액세스 차단)
resource "aws_s3_bucket_public_access_block" "taxi_lake_block" {
  bucket = aws_s3_bucket.taxi_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. Kafka Connect가 사용할 전용 사용자(IAM) 만들기
resource "aws_iam_user" "s3_connector_user" {
  name = "nyc-taxi-s3-sink-connector"
}

# 4. 그 사용자에게 S3 쓰기 권한 주기
resource "aws_iam_user_policy" "s3_write_policy" {
  name = "S3WritePolicy"
  user = aws_iam_user.s3_connector_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:AbortMultipartUpload"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.taxi_lake.arn,
          "${aws_s3_bucket.taxi_lake.arn}/*"
        ]
      }
    ]
  })
}

# 5. 접속용 키 출력하기
resource "aws_iam_access_key" "connector_key" {
  user = aws_iam_user.s3_connector_user.name
}

output "connector_access_key" {
  value = aws_iam_access_key.connector_key.id
}

output "connector_secret_key" {
  value     = aws_iam_access_key.connector_key.secret
  sensitive = true
}