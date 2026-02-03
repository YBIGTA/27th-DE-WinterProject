#!/bin/bash
# Start all services on single machine

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Ensure correct env
./config/use-env.sh single-machine

echo "Starting Kafka..."
cd infra/kafka
docker compose up -d

echo "Waiting for Kafka..."
sleep 15

echo "Starting Ingestor cluster..."
cd "$PROJECT_ROOT/ingestor"
docker compose up -d --build

echo "✓ Single-machine deployment started"
echo "  Kafka: localhost:9092"
echo "  Kafka UI: http://localhost:8090"
echo "  Ingestors: http://localhost:8080"
echo ""
echo "To run Generator:"
echo "  cd generator && ./build/generate"
