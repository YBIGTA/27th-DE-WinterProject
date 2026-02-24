#!/bin/sh
set -e

# Backward compatibility: if TaskManager host IPs are not explicitly set,
# fall back to JobManager host IP (single-host distributed style).
: "${FLINK_TASKMANAGER_1_IP:=${FLINK_IP}}"
: "${FLINK_TASKMANAGER_2_IP:=${FLINK_IP}}"
: "${FLINK_TASKMANAGER_3_IP:=${FLINK_IP}}"

# Render prometheus.yml from template using environment variables
sed \
  -e "s|\${FLINK_IP}|${FLINK_IP}|g" \
  -e "s|\${FLINK_TASKMANAGER_1_IP}|${FLINK_TASKMANAGER_1_IP}|g" \
  -e "s|\${FLINK_TASKMANAGER_2_IP}|${FLINK_TASKMANAGER_2_IP}|g" \
  -e "s|\${FLINK_TASKMANAGER_3_IP}|${FLINK_TASKMANAGER_3_IP}|g" \
  -e "s|\${FLINK_JOBMANAGER_PORT}|${FLINK_JOBMANAGER_PORT}|g" \
  -e "s|\${FLINK_JOBMANAGER_METRICS_PORT:-9249}|${FLINK_JOBMANAGER_METRICS_PORT:-9249}|g" \
  -e "s|\${FLINK_TASKMANAGER_1_METRICS_PORT:-9250}|${FLINK_TASKMANAGER_1_METRICS_PORT:-9250}|g" \
  -e "s|\${FLINK_TASKMANAGER_2_METRICS_PORT:-9251}|${FLINK_TASKMANAGER_2_METRICS_PORT:-9251}|g" \
  -e "s|\${FLINK_TASKMANAGER_3_METRICS_PORT:-9252}|${FLINK_TASKMANAGER_3_METRICS_PORT:-9252}|g" \
  -e "s|\${NGINX_IP}|${NGINX_IP}|g" \
  -e "s|\${NGINX_LB_PORT}|${NGINX_LB_PORT}|g" \
  -e "s|\${NGINX_EXPORTER_PORT:-9113}|${NGINX_EXPORTER_PORT:-9113}|g" \
  -e "s|\${NGINX_PROMTAIL_PORT:-9085}|${NGINX_PROMTAIL_PORT:-9085}|g" \
  -e "s|\${INGESTOR_1_IP}|${INGESTOR_1_IP}|g" \
  -e "s|\${INGESTOR_1_PORT}|${INGESTOR_1_PORT}|g" \
  -e "s|\${INGESTOR_2_IP}|${INGESTOR_2_IP}|g" \
  -e "s|\${INGESTOR_2_PORT}|${INGESTOR_2_PORT}|g" \
  -e "s|\${INGESTOR_3_IP}|${INGESTOR_3_IP}|g" \
  -e "s|\${INGESTOR_3_PORT}|${INGESTOR_3_PORT}|g" \
  -e "s|\${GENERATOR_IP}|${GENERATOR_IP}|g" \
  -e "s|\${GENERATOR_METRICS_PORT:-9108}|${GENERATOR_METRICS_PORT:-9108}|g" \
  -e "s|\${KAFKA_1_IP}|${KAFKA_1_IP}|g" \
  -e "s|\${KAFKA_1_EXTERNAL_PORT}|${KAFKA_1_EXTERNAL_PORT}|g" \
  -e "s|\${KAFKA_1_JMX_EXPORTER_PORT:-9404}|${KAFKA_1_JMX_EXPORTER_PORT:-9404}|g" \
  -e "s|\${KAFKA_2_IP}|${KAFKA_2_IP}|g" \
  -e "s|\${KAFKA_2_EXTERNAL_PORT}|${KAFKA_2_EXTERNAL_PORT}|g" \
  -e "s|\${KAFKA_2_JMX_EXPORTER_PORT:-9405}|${KAFKA_2_JMX_EXPORTER_PORT:-9405}|g" \
  -e "s|\${KAFKA_3_IP}|${KAFKA_3_IP}|g" \
  -e "s|\${KAFKA_3_EXTERNAL_PORT}|${KAFKA_3_EXTERNAL_PORT}|g" \
  -e "s|\${KAFKA_3_JMX_EXPORTER_PORT:-9406}|${KAFKA_3_JMX_EXPORTER_PORT:-9406}|g" \
  -e "s|\${CLICKHOUSE_IP}|${CLICKHOUSE_IP}|g" \
  -e "s|\${CLICKHOUSE_HTTP_PORT:-8123}|${CLICKHOUSE_HTTP_PORT:-8123}|g" \
  -e "s|\${CLICKHOUSE_PROMETHEUS_PORT:-9363}|${CLICKHOUSE_PROMETHEUS_PORT:-9363}|g" \
  -e "s|\${GRAFANA_IP}|${GRAFANA_IP}|g" \
  -e "s|\${GRAFANA_PORT:-3000}|${GRAFANA_PORT:-3000}|g" \
  -e "s|\${LOKI_IP}|${LOKI_IP}|g" \
  -e "s|\${LOKI_PORT:-3100}|${LOKI_PORT:-3100}|g" \
  -e "s|\${PROMTAIL_LOKI_PORT:-9084}|${PROMTAIL_LOKI_PORT:-9084}|g" \
  /etc/prometheus/prometheus.yml.tmpl > /tmp/prometheus.yml

echo "=== Rendered prometheus.yml ==="
cat /tmp/prometheus.yml
echo "==============================="

exec /bin/prometheus \
  --config.file=/tmp/prometheus.yml \
  --storage.tsdb.retention.time=7d
