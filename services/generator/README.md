# Generator Build/Run Guide

## Build
`services/generator`에서 실행:

```bash
uv --project ../../data run conan profile detect --force
uv --project ../../data run conan install . -of build --build=missing

cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Run
```bash
./build/generate
# 또는
./build/generate config/default.yaml
```

Prometheus metrics endpoint:
- 기본: `http://0.0.0.0:9108/metrics`
- 포트 변경: `GENERATOR_METRICS_PORT=<port> ./build/generate ...`

Loki file scrape용 로그 파일 실행 예시:
```bash
mkdir -p data
./build/generate config/default.yaml 2>&1 | tee data/generator.log
```

## 설정 우선순위
1. 환경변수 `INGEST_URL` (있으면 최우선)
2. 환경변수 `NGINX_IP` + `NGINX_LB_PORT` 조합
3. `config/default.yaml`의 `ingestion_url`

Generator는 compose 서비스가 아니며 native binary로 실행한다.
