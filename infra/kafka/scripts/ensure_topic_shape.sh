#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-1:29092,kafka-2:29092,kafka-3:29092}"
TOPIC_NAME="${TOPIC_NAME:-taxi-event-data}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-12}"
TOPIC_REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:-3}"
TOPIC_MIN_INSYNC_REPLICAS="${TOPIC_MIN_INSYNC_REPLICAS:-2}"
TOPIC_ENSURE_TIMEOUT_SEC="${TOPIC_ENSURE_TIMEOUT_SEC:-300}"
TOPIC_ENSURE_RETRY_SEC="${TOPIC_ENSURE_RETRY_SEC:-5}"

log() {
  printf '[TOPIC_INIT] %s\n' "$*"
}

fatal() {
  log "ERROR: $*"
  exit 1
}

deadline=$((SECONDS + TOPIC_ENSURE_TIMEOUT_SEC))

wait_for_bootstrap() {
  while true; do
    if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --list >/dev/null 2>&1; then
      log "Kafka bootstrap reachable: $BOOTSTRAP_SERVERS"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      fatal "Kafka bootstrap is not reachable within timeout=${TOPIC_ENSURE_TIMEOUT_SEC}s"
    fi
    sleep "$TOPIC_ENSURE_RETRY_SEC"
  done
}

get_summary_line() {
  kafka-topics \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe \
    --topic "$TOPIC_NAME" \
    | grep 'PartitionCount:' \
    | head -n1 || true
}

ensure_topic_once() {
  if ! kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$TOPIC_NAME" >/tmp/topic_describe.out 2>/tmp/topic_describe.err; then
    log "Topic not found yet. Creating $TOPIC_NAME with ${TOPIC_PARTITIONS}/${TOPIC_REPLICATION_FACTOR} ..."
    if ! kafka-topics \
      --bootstrap-server "$BOOTSTRAP_SERVERS" \
      --create \
      --if-not-exists \
      --topic "$TOPIC_NAME" \
      --partitions "$TOPIC_PARTITIONS" \
      --replication-factor "$TOPIC_REPLICATION_FACTOR" \
      --config "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}" \
      >/tmp/topic_create.out 2>/tmp/topic_create.err; then
      log "Create failed (will retry): $(tr '\n' ' ' < /tmp/topic_create.err)"
      return 1
    fi
    log "Create completed: $TOPIC_NAME"
  fi

  local summary_line current_partitions current_rf
  summary_line="$(get_summary_line)"
  if [[ -z "$summary_line" ]]; then
    log "Cannot read topic summary yet (will retry)"
    return 1
  fi

  current_partitions="$(echo "$summary_line" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p')"
  current_rf="$(echo "$summary_line" | sed -n 's/.*ReplicationFactor: \([0-9][0-9]*\).*/\1/p')"

  if [[ -z "$current_partitions" || -z "$current_rf" ]]; then
    log "Cannot parse summary line (will retry): $summary_line"
    return 1
  fi

  if (( current_partitions < TOPIC_PARTITIONS )); then
    log "Increasing partitions: $current_partitions -> $TOPIC_PARTITIONS"
    if ! kafka-topics \
      --bootstrap-server "$BOOTSTRAP_SERVERS" \
      --alter \
      --topic "$TOPIC_NAME" \
      --partitions "$TOPIC_PARTITIONS" \
      >/tmp/topic_alter.out 2>/tmp/topic_alter.err; then
      log "Partition alter failed (will retry): $(tr '\n' ' ' < /tmp/topic_alter.err)"
      return 1
    fi
  elif (( current_partitions > TOPIC_PARTITIONS )); then
    fatal "Topic partitions are larger than expected ($current_partitions > $TOPIC_PARTITIONS). Partition shrink is not supported."
  fi

  if (( current_rf != TOPIC_REPLICATION_FACTOR )); then
    fatal "Replication factor mismatch: current=$current_rf expected=$TOPIC_REPLICATION_FACTOR. Reassignment/reset is required."
  fi

  if ! kafka-configs \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --entity-type topics \
    --entity-name "$TOPIC_NAME" \
    --alter \
    --add-config "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}" \
    >/tmp/topic_config.out 2>/tmp/topic_config.err; then
    log "Topic config alter failed (will retry): $(tr '\n' ' ' < /tmp/topic_config.err)"
    return 1
  fi

  summary_line="$(get_summary_line)"
  current_partitions="$(echo "$summary_line" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p')"
  current_rf="$(echo "$summary_line" | sed -n 's/.*ReplicationFactor: \([0-9][0-9]*\).*/\1/p')"

  if [[ "$current_partitions" != "$TOPIC_PARTITIONS" ]]; then
    fatal "Final partition check failed: current=$current_partitions expected=$TOPIC_PARTITIONS"
  fi

  if [[ "$current_rf" != "$TOPIC_REPLICATION_FACTOR" ]]; then
    fatal "Final replication factor check failed: current=$current_rf expected=$TOPIC_REPLICATION_FACTOR"
  fi

  local cfg_desc
  cfg_desc="$(kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$TOPIC_NAME" --describe)"
  if ! echo "$cfg_desc" | grep -q "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}"; then
    fatal "Final min.insync.replicas check failed. expected=${TOPIC_MIN_INSYNC_REPLICAS}"
  fi

  log "Topic shape verified: topic=$TOPIC_NAME partitions=$TOPIC_PARTITIONS rf=$TOPIC_REPLICATION_FACTOR min.insync.replicas=$TOPIC_MIN_INSYNC_REPLICAS"
  return 0
}

wait_for_bootstrap

while true; do
  if ensure_topic_once; then
    exit 0
  fi

  if (( SECONDS >= deadline )); then
    fatal "Timed out while ensuring topic shape."
  fi
  sleep "$TOPIC_ENSURE_RETRY_SEC"
done
