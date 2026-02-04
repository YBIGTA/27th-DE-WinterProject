#!/bin/sh
# Broker tuning: default.yaml (flat KAFKA_* keys) -> env vars.
# Confluent's run script converts KAFKA_* env vars -> kafka.* properties.

awk -F': ' '/^[A-Z][A-Z0-9_]*:/{v=$2; gsub(/^"|"$/, "", v); print $1 "=\"" v "\""}' /config/default.yaml > /tmp/tuning.env
set -a
. /tmp/tuning.env
set +a

exec /etc/confluent/docker/run
