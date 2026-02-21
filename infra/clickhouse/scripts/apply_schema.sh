#!/usr/bin/env bash
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
SCHEMA_FILE="${SCHEMA_FILE:-/schema.sql}"
CLICKHOUSE_SCHEMA_WAIT_TIMEOUT_SEC="${CLICKHOUSE_SCHEMA_WAIT_TIMEOUT_SEC:-180}"
CLICKHOUSE_SCHEMA_RETRY_SEC="${CLICKHOUSE_SCHEMA_RETRY_SEC:-2}"
CLICKHOUSE_SCHEMA_BACKFILL="${CLICKHOUSE_SCHEMA_BACKFILL:-false}"

log() {
  printf '[SCHEMA_SYNC] %s\n' "$*"
}

fatal() {
  log "ERROR: $*"
  exit 1
}

deadline=$((SECONDS + CLICKHOUSE_SCHEMA_WAIT_TIMEOUT_SEC))

wait_for_clickhouse() {
  while true; do
    if clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" -q "SELECT 1" >/dev/null 2>&1; then
      log "ClickHouse reachable: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      fatal "ClickHouse is not reachable within timeout=${CLICKHOUSE_SCHEMA_WAIT_TIMEOUT_SEC}s"
    fi
    sleep "$CLICKHOUSE_SCHEMA_RETRY_SEC"
  done
}

should_backfill() {
  case "$(echo "$CLICKHOUSE_SCHEMA_BACKFILL" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) return 0 ;;
    *) return 1 ;;
  esac
}

apply_schema() {
  if [[ ! -f "$SCHEMA_FILE" ]]; then
    fatal "Schema file not found: $SCHEMA_FILE"
  fi
  clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --multiquery < "$SCHEMA_FILE"
  log "Schema applied from $SCHEMA_FILE"
}

backfill_serving_tables() {
  clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --multiquery <<'SQL'
INSERT INTO default.taxi_events_serving
SELECT trip_id, ts, zone_id, event
FROM default.taxi_events;

INSERT INTO default.taxi_predictions_serving
SELECT prediction_time, target_time, zone_id, predicted_demand, model_version
FROM default.taxi_predictions;
SQL
  log "Serving table backfill completed"
}

wait_for_clickhouse
apply_schema

if should_backfill; then
  backfill_serving_tables
else
  log "Backfill skipped (set CLICKHOUSE_SCHEMA_BACKFILL=true to enable)"
fi
