# Generator Build/Run Guide

## Build
`services/generator`에서 실행:

```bash
uv run conan profile detect --force
uv run conan install . -of build --build=missing

cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Run
```bash
./build/generate
# 또는
./build/generate config/default.yaml
```

## 설정 우선순위
1. 환경변수 `INGEST_URL` (있으면 최우선)
2. 환경변수 `NGINX_IP` + `NGINX_LB_PORT` 조합
3. `config/default.yaml`의 `ingestion_url`

Generator는 compose 서비스가 아니며 native binary로 실행한다.
