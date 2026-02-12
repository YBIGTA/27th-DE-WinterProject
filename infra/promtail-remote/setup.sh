#!/bin/bash
set -e

LOKI_URL="${LOKI_URL:-http://100.98.239.46:3100}"

echo "=== Loki Docker Logging Driver Setup ==="
echo "Loki URL: $LOKI_URL"
echo ""

# 1. Install Loki Docker plugin
if docker plugin ls | grep -q loki; then
  echo "[OK] Loki plugin already installed"
else
  echo "[*] Installing Loki Docker plugin..."
  docker plugin install grafana/loki-docker-driver:2.9.1 --alias loki --grant-all-permissions
  echo "[OK] Plugin installed"
fi

# 2. Configure daemon.json
DAEMON_JSON="/etc/docker/daemon.json"
HOSTNAME=$(hostname)

if [ -f "$DAEMON_JSON" ]; then
  echo "[!] $DAEMON_JSON already exists. Current contents:"
  cat "$DAEMON_JSON"
  echo ""
  read -p "Overwrite? (y/N): " confirm
  if [ "$confirm" != "y" ]; then
    echo "Aborted."
    exit 1
  fi
fi

echo "[*] Writing $DAEMON_JSON..."
sudo tee "$DAEMON_JSON" > /dev/null <<EOF
{
  "log-driver": "loki",
  "log-opts": {
    "loki-url": "$LOKI_URL/loki/api/v1/push",
    "loki-batch-size": "400",
    "loki-external-labels": "host=$HOSTNAME,job=docker"
  }
}
EOF
echo "[OK] daemon.json configured"

# 3. Restart Docker
echo "[*] Restarting Docker daemon..."
sudo systemctl restart docker
echo "[OK] Docker restarted"

echo ""
echo "=== Done ==="
echo "All new containers will send logs to $LOKI_URL"
echo "Existing containers need to be recreated (docker compose up -d --force-recreate)"
echo ""
echo "Verify in Grafana: {job=\"docker\", host=\"$HOSTNAME\"}"
