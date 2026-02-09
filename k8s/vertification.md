# Phase 1.5 Vertification Runbook (Multi-Node)

## A) What Phase 1.5 Means

Phase 1.5 is the step between local `k3d` testing and full production-style rollout.

Goal:
- Keep the existing Kubernetes manifests from Phase 1.
- Change only the cluster runtime from single-machine `k3d` to multi-machine `k3s`.
- Use Tailscale as the cross-machine network so Linux/Mac/Windows nodes can join one cluster.

Architecture summary:
- Each machine runs one Docker container based on `rancher/k3s`.
- Tailscale runs inside each container and gives it a mesh IP.
- One container runs as `server` (control plane), others run as `agent` (workers).

## B) Current Completion Status (as of 2026-02-09)

Completed items:

| ID | Status | What is done exactly |
|---|---|---|
| P15-01 | DONE | Created `k8s/multi-node/` and phase-specific runtime asset structure. |
| P15-02 | DONE | Added `k8s/multi-node/Dockerfile` using `rancher/k3s:v1.34.3-k3s1` and bundling `tailscaled`/`tailscale` binaries. |
| P15-03 | DONE | Added `k8s/multi-node/entrypoint.sh` with role split (`K3S_ROLE=server|agent`), Tailscale auth flow, and k3s startup logic. |
| P15-04 | DONE | Added `k8s/multi-node/README.md` with build/run/join/kubeconfig validation instructions. |
| P15-07 | DONE | Documented Tailscale-reachable registry strategy in `k8s/multi-node/README.md`. |

Not completed yet:

| ID | Status | Remaining work |
|---|---|---|
| P15-05 | TODO | Run server container and confirm live join to Tailscale + k3s API exposure. |
| P15-06 | TODO | Run agent container(s) and confirm cluster join. |
| P15-08 | TODO | Apply existing Phase 1 manifests on the Phase 1.5 cluster and confirm pods run. |

This runbook is for manual verification of those remaining items (`P15-05`, `P15-06`, `P15-08`).

## 0) Session Record

- Date:
- Operator:
- Server machine:
- Agent machine(s):
- Result summary: `PASS` / `FAIL`

## 1) Preflight

- [ ] Docker is available on all participating machines.
- [ ] Tailscale auth key is ready (`TS_AUTHKEY`).
- [ ] Shared cluster token is ready (`K3S_TOKEN`).
- [ ] `kubectl` is installed on the machine used for control-plane checks.

Commands:

```bash
docker version
kubectl version --client
```

## 2) Build Phase 1.5 Image

- [ ] Build succeeds on at least one machine.

```bash
docker build -t k3s-tailscale:latest -f k8s/multi-node/Dockerfile k8s/multi-node
```

Expected:
- Image `k3s-tailscale:latest` exists.

## 3) Start Server Container (P15-05)

- [ ] Server container starts without crash loop.
- [ ] Server joins Tailscale and gets IPv4.
- [ ] k3s server listens on `:6443`.

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

Checks:

```bash
docker ps --filter name=k3s-ts-server
docker logs --tail=200 k3s-ts-server
docker exec k3s-ts-server tailscale ip -4
```

Notes:
- Record server Tailscale IP here: `________________`

## 4) Start Agent Container (P15-06)

- [ ] Agent container starts.
- [ ] Agent can reach server using `K3S_URL=https://<server-ts-ip>:6443`.
- [ ] Agent joins cluster successfully.

```bash
docker run -d \
  --name k3s-ts-agent-1 \
  --hostname k3s-ts-agent-1 \
  --privileged \
  -e K3S_ROLE=agent \
  -e TS_AUTHKEY=tskey-your-key \
  -e K3S_TOKEN=taxi-cluster-shared-token \
  -e K3S_URL=https://<server-ts-ip>:6443 \
  -e TS_HOSTNAME=taxi-k3s-agent-1 \
  -v k3s-agent1-data:/var/lib/rancher/k3s \
  -v ts-agent1-state:/var/lib/tailscale \
  k3s-tailscale:latest
```

Checks:

```bash
docker ps --filter name=k3s-ts-agent-1
docker logs --tail=200 k3s-ts-agent-1
docker exec k3s-ts-agent-1 tailscale ip -4
```

## 5) Validate Cluster Join From kubectl

- [ ] kubeconfig exported from server container.
- [ ] kubeconfig server endpoint replaced with server Tailscale IP.
- [ ] `kubectl get nodes` shows control-plane + agent nodes as `Ready`.

```bash
docker cp k3s-ts-server:/etc/rancher/k3s/k3s.yaml /tmp/k3s-phase15.yaml
# edit /tmp/k3s-phase15.yaml: replace 127.0.0.1 with <server-ts-ip>
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl get nodes -o wide
```

Expected:
- Node count >= 2
- All nodes in `Ready`

## 6) Registry Reachability Check

- [ ] Registry runs on Tailscale-reachable host.
- [ ] Push and pull succeed from the chosen endpoint.

```bash
docker run -d --restart unless-stopped --name taxi-registry -p 5000:5000 registry:2
docker tag k3s-tailscale:latest <server-ts-ip>:5000/k3s-tailscale:latest
docker push <server-ts-ip>:5000/k3s-tailscale:latest
```

Expected:
- Push completes without network/auth errors.

## 7) Manifest Reuse Check (P15-08)

- [ ] Existing Phase 1 manifests apply on Phase 1.5 cluster.
- [ ] Pods become `Running` in `taxi-pipeline` namespace.

```bash
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl apply -f k8s/namespace.yaml
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl apply -f k8s/kafka/
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl apply -f k8s/ingestor/
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl apply -f k8s/flink/
KUBECONFIG=/tmp/k3s-phase15.yaml kubectl get pods -n taxi-pipeline
```

Expected:
- No manifest schema errors.
- Core pods start (Kafka/Ingestor/Flink).

## 8) Result Log

- P15-05 (server path): `PASS / FAIL`
- P15-06 (agent join): `PASS / FAIL`
- P15-08 (manifest reuse): `PASS / FAIL`
- Blocking issue(s):
- Next action:

## 9) Cleanup

```bash
docker rm -f k3s-ts-agent-1 k3s-ts-server || true
docker rm -f taxi-registry || true
docker volume rm k3s-server-data ts-server-state k3s-agent1-data ts-agent1-state || true
```
