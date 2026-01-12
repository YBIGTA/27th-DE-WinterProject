# Conan (recommended)
# from generator/
conan profile detect --force
conan install . -of build --build=missing
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
cmake --build build
./build/generate
