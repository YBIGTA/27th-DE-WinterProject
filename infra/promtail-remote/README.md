# Remote Log Collection

원격 머신의 Docker 컨테이너 로그를 중앙 Loki 서버로 전송합니다.
두 가지 방식 중 하나를 선택하세요.

## 방법 1: Promtail (권장)

원격 머신에 Promtail 컨테이너를 띄워서 로그를 수집합니다.

```bash
# 이 디렉토리를 원격 머신에 복사
scp -r infra/promtail-remote/ <remote>:~/promtail-remote/

# 원격 머신에서 실행
cd ~/promtail-remote
HOSTNAME=$(hostname) docker compose up -d
```

Loki 주소가 다른 경우:

```bash
LOKI_URL=http://<tailscale-ip>:3100 HOSTNAME=$(hostname) docker compose up -d
```

| 환경 변수 | 기본값 | 설명 |
|-----------|--------|------|
| `LOKI_URL` | `http://100.98.239.46:3100` | Loki 서버 Tailscale IP |
| `HOSTNAME` | `unknown` | Grafana에서 호스트 구분용 라벨 |

## 방법 2: Loki Docker Driver

Docker 자체 로깅 드라이버를 사용합니다. Promtail 컨테이너가 필요 없습니다.

### Linux

```bash
chmod +x setup.sh && ./setup.sh
```

### macOS / Windows (Docker Desktop)

```bash
# 1. 플러그인 설치
docker plugin install grafana/loki-docker-driver:2.9.1 --alias loki --grant-all-permissions
```

2. Docker Desktop → Settings → Docker Engine에 아래 JSON 추가:

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

3. **Apply & Restart** 클릭

> 기존 컨테이너는 `docker compose up -d --force-recreate`로 재생성해야 합니다.

## 확인

Grafana(`http://100.98.239.46:3000`) Explore에서:

```logql
# Promtail 방식
{host="<hostname>"}

# Docker Driver 방식
{job="docker", host="<내-이름>"}
```

## 롤백

- **Promtail**: `docker compose down`
- **Docker Driver**: daemon.json에서 `log-driver`, `log-opts` 제거 후 Docker 재시작
