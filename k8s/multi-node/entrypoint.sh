#!/bin/sh
set -eu

log() {
  printf '%s %s\n' "[k3s-ts-entrypoint]" "$*"
}

require_env() {
  var_name="$1"
  eval "var_value=\${$var_name:-}"
  if [ -z "$var_value" ]; then
    log "ERROR: required env is missing: $var_name"
    exit 1
  fi
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "ERROR: required command not found: $1"
    exit 1
  fi
}

require_cmd tailscaled
require_cmd tailscale
require_cmd k3s

require_env K3S_ROLE
require_env TS_AUTHKEY
require_env K3S_TOKEN

K3S_ROLE="$(printf '%s' "$K3S_ROLE" | tr '[:upper:]' '[:lower:]')"
case "$K3S_ROLE" in
  server|agent) ;;
  *)
    log "ERROR: K3S_ROLE must be either 'server' or 'agent' (current: $K3S_ROLE)"
    exit 1
    ;;
esac

TS_STATE_DIR="${TS_STATE_DIR:-/var/lib/tailscale}"
TS_STATE_FILE="${TS_STATE_FILE:-${TS_STATE_DIR}/tailscaled.state}"
TS_SOCKET="${TS_SOCKET:-/run/tailscale/tailscaled.sock}"
TS_HOSTNAME="${TS_HOSTNAME:-taxi-${K3S_ROLE}-$(hostname)}"
TS_ACCEPT_DNS="${TS_ACCEPT_DNS:-false}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-true}"

K3S_DATA_DIR="${K3S_DATA_DIR:-/var/lib/rancher/k3s}"
K3S_DISABLE_TRAEFIK="${K3S_DISABLE_TRAEFIK:-true}"

mkdir -p "$TS_STATE_DIR" "$(dirname "$TS_SOCKET")" "$K3S_DATA_DIR"
rm -f "$TS_SOCKET"

log "Starting tailscaled (state=${TS_STATE_FILE}, socket=${TS_SOCKET})"
tailscaled \
  --state="$TS_STATE_FILE" \
  --socket="$TS_SOCKET" \
  --tun=tailscale0 &

for i in $(seq 1 30); do
  if [ -S "$TS_SOCKET" ]; then
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    log "ERROR: tailscaled socket did not become ready in time"
    exit 1
  fi
done

set -- \
  --authkey="$TS_AUTHKEY" \
  --hostname="$TS_HOSTNAME" \
  --accept-dns="$TS_ACCEPT_DNS" \
  --accept-routes="$TS_ACCEPT_ROUTES" \
  --reset

if [ -n "${TS_ADVERTISE_TAGS:-}" ]; then
  set -- "$@" --advertise-tags="$TS_ADVERTISE_TAGS"
fi

log "Bringing up tailscale identity (hostname=${TS_HOSTNAME})"
tailscale --socket="$TS_SOCKET" up "$@"

TS_IP=""
for i in $(seq 1 45); do
  TS_IP="$(tailscale --socket="$TS_SOCKET" ip -4 2>/dev/null | head -n1 || true)"
  if [ -n "$TS_IP" ]; then
    break
  fi
  sleep 1
  if [ "$i" -eq 45 ]; then
    log "ERROR: failed to resolve tailscale IPv4 address"
    exit 1
  fi
done

log "Tailscale IPv4: ${TS_IP}"

if [ "$K3S_ROLE" = "server" ]; then
  log "Starting k3s server"
  set -- \
    server \
    --node-ip="$TS_IP" \
    --node-external-ip="$TS_IP" \
    --advertise-address="$TS_IP" \
    --flannel-iface=tailscale0 \
    --tls-san="$TS_IP" \
    --token="$K3S_TOKEN" \
    --write-kubeconfig-mode=644 \
    --data-dir="$K3S_DATA_DIR"

  if [ "$K3S_DISABLE_TRAEFIK" = "true" ]; then
    set -- "$@" --disable=traefik
  fi

  if [ -n "${K3S_SERVER_EXTRA_ARGS:-}" ]; then
    # shellcheck disable=SC2086
    set -- "$@" ${K3S_SERVER_EXTRA_ARGS}
  fi

  exec k3s "$@"
fi

require_env K3S_URL
log "Starting k3s agent -> ${K3S_URL}"

set -- \
  agent \
  --server="$K3S_URL" \
  --token="$K3S_TOKEN" \
  --node-ip="$TS_IP" \
  --node-external-ip="$TS_IP" \
  --flannel-iface=tailscale0 \
  --data-dir="$K3S_DATA_DIR"

if [ -n "${K3S_AGENT_EXTRA_ARGS:-}" ]; then
  # shellcheck disable=SC2086
  set -- "$@" ${K3S_AGENT_EXTRA_ARGS}
fi

exec k3s "$@"
