#!/bin/sh
set -e

# Render prometheus.yml from template using environment variables
sed \
  -e "s|\${FLINK_IP}|${FLINK_IP}|g" \
  -e "s|\${FLINK_JOBMANAGER_PORT}|${FLINK_JOBMANAGER_PORT}|g" \
  -e "s|\${NGINX_IP}|${NGINX_IP}|g" \
  -e "s|\${NGINX_LB_PORT}|${NGINX_LB_PORT}|g" \
  -e "s|\${INGESTOR_1_IP}|${INGESTOR_1_IP}|g" \
  -e "s|\${INGESTOR_1_PORT}|${INGESTOR_1_PORT}|g" \
  -e "s|\${INGESTOR_2_IP}|${INGESTOR_2_IP}|g" \
  -e "s|\${INGESTOR_2_PORT}|${INGESTOR_2_PORT}|g" \
  -e "s|\${INGESTOR_3_IP}|${INGESTOR_3_IP}|g" \
  -e "s|\${INGESTOR_3_PORT}|${INGESTOR_3_PORT}|g" \
  -e "s|\${KAFKA_1_IP}|${KAFKA_1_IP}|g" \
  -e "s|\${KAFKA_1_EXTERNAL_PORT}|${KAFKA_1_EXTERNAL_PORT}|g" \
  -e "s|\${KAFKA_2_IP}|${KAFKA_2_IP}|g" \
  -e "s|\${KAFKA_2_EXTERNAL_PORT}|${KAFKA_2_EXTERNAL_PORT}|g" \
  -e "s|\${KAFKA_3_IP}|${KAFKA_3_IP}|g" \
  -e "s|\${KAFKA_3_EXTERNAL_PORT}|${KAFKA_3_EXTERNAL_PORT}|g" \
  -e "s|\${CLICKHOUSE_IP}|${CLICKHOUSE_IP}|g" \
  /etc/prometheus/prometheus.yml.tmpl > /tmp/prometheus.yml

echo "=== Rendered prometheus.yml ==="
cat /tmp/prometheus.yml
echo "==============================="

exec /bin/prometheus \
  --config.file=/tmp/prometheus.yml \
  --storage.tsdb.retention.time=7d
