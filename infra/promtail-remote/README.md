# Remote Log Collection (Loki Docker Driver)

원격 머신의 Docker 컨테이너 로그를 중앙 Loki로 전송합니다.
Promtail 없이 Docker 자체 로깅 드라이버를 사용합니다.

## 사용법

```bash
# Tailscale 연결 확인 후:
chmod +x setup.sh
./setup.sh
```

Loki 주소를 변경하려면:

```bash
LOKI_URL=http://<tailscale-ip>:3100 ./setup.sh
```

## 설정 후

- **새 컨테이너**: 자동으로 로그가 Loki로 전송됩니다.
- **기존 컨테이너**: `docker compose up -d --force-recreate`로 재생성해야 합니다.

## 확인

Grafana에서 `host` 라벨로 머신별 로그를 구분할 수 있습니다.

```logql
{job="docker", host="my-machine"}
```

## 롤백

```bash
sudo rm /etc/docker/daemon.json
sudo systemctl restart docker
```
