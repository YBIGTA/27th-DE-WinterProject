cmake_minimum_required(VERSION 3.15)
project(UberTrafficSim)

set(CMAKE_CXX_STANDARD 17)

# 시스템 cmake 경로 무시
list(REMOVE_ITEM CMAKE_SYSTEM_PREFIX_PATH "/opt/homebrew")
set(CMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY FALSE)
set(CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY TRUE)
set(CMAKE_FIND_PACKAGE_NO_SYSTEM_PACKAGE_REGISTRY TRUE)

# Conan에서 생성한 Arrow 패키지 (Parquet 포함)
find_package(Arrow REQUIRED)

add_executable(generate generate.cpp)

# Arrow 타겟 (Parquet 포함)
if (TARGET arrow::arrow)
  target_link_libraries(generate PRIVATE arrow::arrow)
elseif (TARGET Arrow::arrow_shared)
  target_link_libraries(generate PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
else()
  message(FATAL_ERROR "Arrow target not found")
endif()
