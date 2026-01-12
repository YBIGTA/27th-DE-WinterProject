cmake_minimum_required(VERSION 3.15)
project(UberTrafficSim)

set(CMAKE_CXX_STANDARD 17)

# Arrow 라이브러리 찾기 (설치된 경로 자동 탐색)
find_package(Arrow REQUIRED)
find_package(Parquet QUIET)

add_executable(generate generate.cpp)

set(ARROW_TARGET "")
if (TARGET Arrow::arrow_shared)
  set(ARROW_TARGET Arrow::arrow_shared)
elseif (TARGET Arrow::arrow)
  set(ARROW_TARGET Arrow::arrow)
elseif (TARGET arrow::arrow_shared)
  set(ARROW_TARGET arrow::arrow_shared)
elseif (TARGET arrow::arrow)
  set(ARROW_TARGET arrow::arrow)
endif()

set(PARQUET_TARGET "")
if (TARGET Parquet::parquet_shared)
  set(PARQUET_TARGET Parquet::parquet_shared)
elseif (TARGET Parquet::parquet)
  set(PARQUET_TARGET Parquet::parquet)
elseif (TARGET Arrow::parquet_shared)
  set(PARQUET_TARGET Arrow::parquet_shared)
elseif (TARGET Arrow::parquet)
  set(PARQUET_TARGET Arrow::parquet)
elseif (TARGET parquet::parquet)
  set(PARQUET_TARGET parquet::parquet)
endif()

if (NOT ARROW_TARGET)
  message(FATAL_ERROR "Arrow target not found. Ensure Arrow is installed and discoverable.")
endif()
if (NOT PARQUET_TARGET)
  message(FATAL_ERROR "Parquet target not found. Ensure Parquet is installed or built with Arrow.")
endif()

# 라이브러리 링크
target_link_libraries(generate PRIVATE ${ARROW_TARGET} ${PARQUET_TARGET})
