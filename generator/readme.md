# Conan (recommended)
# from generator/
uv run conan profile detect --force

uv run conan install . -of build --build=missing

cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release

(start from here when rebuilding)
cmake --build build 

./build/generate
