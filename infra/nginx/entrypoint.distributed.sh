#!/bin/sh
set -eu

# Optional explicit overrides take precedence.
# If override is absent and ingestor IP == NGINX_IP, route via container DNS on local docker network.
# Otherwise route via configured host IP:published port.

if [ -z "${INGESTOR_1_UPSTREAM_HOST:-}" ]; then
  if [ "${INGESTOR_1_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_1_UPSTREAM_HOST="ingestor-1"
  else
    INGESTOR_1_UPSTREAM_HOST="${INGESTOR_1_IP:-}"
  fi
fi
if [ -z "${INGESTOR_1_UPSTREAM_PORT:-}" ]; then
  if [ "${INGESTOR_1_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_1_UPSTREAM_PORT="${INGESTOR_PORT:-8080}"
  else
    INGESTOR_1_UPSTREAM_PORT="${INGESTOR_1_PORT:-8081}"
  fi
fi

if [ -z "${INGESTOR_2_UPSTREAM_HOST:-}" ]; then
  if [ "${INGESTOR_2_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_2_UPSTREAM_HOST="ingestor-2"
  else
    INGESTOR_2_UPSTREAM_HOST="${INGESTOR_2_IP:-}"
  fi
fi
if [ -z "${INGESTOR_2_UPSTREAM_PORT:-}" ]; then
  if [ "${INGESTOR_2_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_2_UPSTREAM_PORT="${INGESTOR_PORT:-8080}"
  else
    INGESTOR_2_UPSTREAM_PORT="${INGESTOR_2_PORT:-8082}"
  fi
fi

if [ -z "${INGESTOR_3_UPSTREAM_HOST:-}" ]; then
  if [ "${INGESTOR_3_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_3_UPSTREAM_HOST="ingestor-3"
  else
    INGESTOR_3_UPSTREAM_HOST="${INGESTOR_3_IP:-}"
  fi
fi
if [ -z "${INGESTOR_3_UPSTREAM_PORT:-}" ]; then
  if [ "${INGESTOR_3_IP:-}" = "${NGINX_IP:-}" ]; then
    INGESTOR_3_UPSTREAM_PORT="${INGESTOR_PORT:-8080}"
  else
    INGESTOR_3_UPSTREAM_PORT="${INGESTOR_3_PORT:-8083}"
  fi
fi

export INGESTOR_1_UPSTREAM_HOST INGESTOR_2_UPSTREAM_HOST INGESTOR_3_UPSTREAM_HOST
export INGESTOR_1_UPSTREAM_PORT INGESTOR_2_UPSTREAM_PORT INGESTOR_3_UPSTREAM_PORT

echo "[nginx-lb] upstream-1=${INGESTOR_1_UPSTREAM_HOST}:${INGESTOR_1_UPSTREAM_PORT}"
echo "[nginx-lb] upstream-2=${INGESTOR_2_UPSTREAM_HOST}:${INGESTOR_2_UPSTREAM_PORT}"
echo "[nginx-lb] upstream-3=${INGESTOR_3_UPSTREAM_HOST}:${INGESTOR_3_UPSTREAM_PORT}"

envsubst '$INGESTOR_1_UPSTREAM_HOST $INGESTOR_2_UPSTREAM_HOST $INGESTOR_3_UPSTREAM_HOST $INGESTOR_1_UPSTREAM_PORT $INGESTOR_2_UPSTREAM_PORT $INGESTOR_3_UPSTREAM_PORT' \
  < /etc/nginx/templates/nginx.conf.template \
  > /etc/nginx/nginx.conf

exec nginx -g 'daemon off;'
