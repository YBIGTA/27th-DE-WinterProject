# Phase 1.5: Multi-Machine k3s with Tailscale (Dockerized)

This directory contains the runtime assets for Phase 1.5:

- `Dockerfile`: `rancher/k3s` + Tailscale binaries
- `entrypoint.sh`: starts `tailscaled`, authenticates, then runs `k3s` as `server` or `agent`

The goal is to reuse the existing manifests under `k8s/` on a cluster that spans multiple machines.

## 1) Prerequisites

- Docker installed on each machine (Linux/Mac/Windows)
- A reusable Tailscale auth key with tag permission (recommended)
- One shared `K3S_TOKEN` value for all nodes
- `kubectl` on the machine that will operate the cluster

## 2) Build Image

Run from repo root:

```bash
docker build -t k3s-tailscale:latest -f k8s/multi-node/Dockerfile k8s/multi-node
```

## 3) Required Environment Contract

- `K3S_ROLE`: `server` or `agent`
- `TS_AUTHKEY`: Tailscale auth key (`tskey-...`)
- `K3S_TOKEN`: shared cluster join token
- `K3S_URL`: required only for `agent` (example: `https://100.x.y.z:6443`)

Optional:

- `TS_HOSTNAME` (default: `taxi-<role>-<container-hostname>`)
- `TS_ADVERTISE_TAGS` (example: `tag:k3s`)
- `K3S_SERVER_EXTRA_ARGS`
- `K3S_AGENT_EXTRA_ARGS`

## 4) Start Controller Node (Server)

```bash
docker run -d \
  --name k3s-ts-server \
  --hostname k3s-ts-server \
  --privileged \
  -p 6443:6443 \
  -e K3S_ROLE=server \
  -e TS_AUTHKEY=tskey-your-key \
  -e K3S_TOKEN=taxi-cluster-shared-token \
  -e TS_HOSTNAME=taxi-k3s-server \
  -v k3s-server-data:/var/lib/rancher/k3s \
  -v ts-server-state:/var/lib/tailscale \
  k3s-tailscale:latest
```

Check logs:

```bash
docker logs -f k3s-ts-server
```

Get server Tailscale IP:

```bash
SERVER_TS_IP="$(docker exec k3s-ts-server tailscale ip -4 | head -n1)"
echo "$SERVER_TS_IP"
```

## 5) Start Worker Node (Agent)

Run this on each worker machine (or locally for smoke tests):

```bash
docker run -d \
  --name k3s-ts-agent-1 \
  --hostname k3s-ts-agent-1 \
  --privileged \
  -e K3S_ROLE=agent \
  -e TS_AUTHKEY=tskey-your-key \
  -e K3S_TOKEN=taxi-cluster-shared-token \
  -e K3S_URL=https://<server-tailscale-ip>:6443 \
  -e TS_HOSTNAME=taxi-k3s-agent-1 \
  -v k3s-agent1-data:/var/lib/rancher/k3s \
  -v ts-agent1-state:/var/lib/tailscale \
  k3s-tailscale:latest
```

## 6) Validate Cluster Join

Copy kubeconfig from server container:

```bash
docker cp k3s-ts-server:/etc/rancher/k3s/k3s.yaml /tmp/k3s-phase15.yaml
```

Replace `127.0.0.1` with server Tailscale IP in `/tmp/k3s-phase15.yaml`, then:

```bash
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl get nodes -o wide
```

Expected: one control-plane node + joined agents.

## 7) Registry Strategy (Phase 1.5)

Do not rely on the k3d built-in registry in Phase 1.5.

Use one Tailscale-reachable registry endpoint (example on server node):

```bash
docker run -d --restart unless-stopped \
  --name taxi-registry \
  -p 5000:5000 \
  registry:2
```

Then push/pull images via:

- `http://<server-tailscale-ip>:5000/taxi-ingestor:latest`
- `http://<server-tailscale-ip>:5000/taxi-flink-job:latest`

If needed, configure k3s `registries.yaml` to trust this endpoint as insecure registry.

## 8) Notes

- `--privileged` is intentionally used for k3s + networking requirements.
- Existing manifests under `k8s/` are expected to be reusable after image registry endpoint updates.
