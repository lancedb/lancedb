# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

cmake_minimum_required(VERSION 3.15)
project(lancedb-c VERSION 0.21.2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(RUST_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../target/debug")
  set(RUST_BUILD_TYPE "")
else()
  set(RUST_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../target/release")
  set(RUST_BUILD_TYPE "--release")
endif()
set(RUST_LIB_PATH "${RUST_TARGET_DIR}/liblancedb.so")

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ARROW REQUIRED arrow)

# rust-lib target
add_custom_target(rust-lib
  COMMAND cargo build ${RUST_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Rust library..."
  BYPRODUCTS ${RUST_LIB_PATH}
)

# lancedb target
add_library(lancedb SHARED IMPORTED)
set_target_properties(lancedb PROPERTIES
  IMPORTED_LOCATION ${RUST_LIB_PATH}
)
add_dependencies(lancedb rust-lib)

# simple example
add_executable(simple examples/simple.cpp)
target_link_libraries(simple
  lancedb
  Threads::Threads
  ${ARROW_LIBRARIES}
)
target_include_directories(simple
  PRIVATE ${ARROW_INCLUDE_DIRS}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(simple PRIVATE ${ARROW_CFLAGS_OTHER})
set_target_properties(simple PROPERTIES
  BUILD_RPATH ${RUST_TARGET_DIR}
)

# full example
add_executable(full examples/full.cpp)
target_link_libraries(full
  lancedb
  Threads::Threads
  ${ARROW_LIBRARIES}
)
target_include_directories(full
  PRIVATE ${ARROW_INCLUDE_DIRS}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(full PRIVATE ${ARROW_CFLAGS_OTHER})
set_target_properties(full PROPERTIES
  BUILD_RPATH ${RUST_TARGET_DIR}
)
add_custom_target(examples DEPENDS simple full)

add_custom_target(clean-all
  COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target clean
  COMMAND cargo clean
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Cleaning all build artifacts..."
)

message(STATUS "=== LanceDB C/C++ Bindings Configuration ===")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Rust library path: ${RUST_LIB_PATH}")
message(STATUS "Arrow library path: ${ARROW_LIBRARIES}")
message(STATUS "============================================")
