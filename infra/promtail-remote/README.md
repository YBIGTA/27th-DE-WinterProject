# Remote Log Collection (Loki Docker Driver)

원격 머신의 Docker 컨테이너 로그를 중앙 Loki로 전송합니다.
Promtail 없이 Docker 자체 로깅 드라이버를 사용합니다.

## Linux

```bash
chmod +x setup.sh && ./setup.sh
```

Loki 주소를 변경하려면:

```bash
LOKI_URL=http://<tailscale-ip>:3100 ./setup.sh
```

## macOS / Windows (Docker Desktop)

```bash
# 1. 플러그인 설치
docker plugin install grafana/loki-docker-driver:2.9.1 --alias loki --grant-all-permissions
```

1. Docker Desktop → Settings → Docker Engine에 아래 JSON 추가:

```json
{
  "log-driver": "loki",
  "log-opts": {
    "loki-url": "http://100.98.239.46:3100/loki/api/v1/push",
    "loki-batch-size": "400",
    "loki-external-labels": "host=<내-이름>,job=docker"
  }
}
```

1. **Apply & Restart** 클릭

## 설정 후

- **새 컨테이너**: 자동으로 로그가 Loki로 전송됩니다.
- **기존 컨테이너**: `docker compose up -d --force-recreate`로 재생성해야 합니다.

## 확인

Grafana(`http://100.98.239.46:3000`)에서 host 라벨로 머신별 로그를 구분할 수 있습니다.

```logql
{job="docker", host="<내-이름>"}
```

## 롤백

daemon.json에서 `log-driver`, `log-opts` 항목을 제거하고 Docker 재시작.
