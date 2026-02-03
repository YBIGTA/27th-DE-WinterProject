#!/bin/bash
# Guide for distributed deployment

cat <<'EOF'
DISTRIBUTED DEPLOYMENT GUIDE (Version 2)

Prerequisites:
1. Edit config/.env.distributed and set actual machine IPs
2. Copy config/.env.distributed to ALL machines as .env
3. Each machine needs the project code

Deployment Steps:

## Machine E (Kafka - 192.168.1.50):
cd /path/to/project/infra/kafka
docker compose up -d

## Machine B (Ingestor-1 - 192.168.1.20):
cd /path/to/project/ingestor
docker compose -f docker-compose.ingestor-1.yml up -d --build

## Machine C (Ingestor-2 - 192.168.1.30):
cd /path/to/project/ingestor
docker compose -f docker-compose.ingestor-2.yml up -d --build

## Machine D (Ingestor-3 - 192.168.1.40):
cd /path/to/project/ingestor
docker compose -f docker-compose.ingestor-3.yml up -d --build

## Machine A (Generator + Nginx - 192.168.1.10):
# Edit ingestor/nginx.distributed.conf with actual IPs first!
cd /path/to/project/ingestor
docker compose -f docker-compose.nginx.yml up -d

# Run Generator
cd /path/to/project/generator
./build/generate

Verification:
- Check Kafka: kafka-console-consumer --bootstrap-server 192.168.1.50:9092 --topic taxi-event-data
- Check Nginx: curl http://localhost:8080/health
- Check each ingestor: curl http://192.168.1.20:8080/health
EOF
