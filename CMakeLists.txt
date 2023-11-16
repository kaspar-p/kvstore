cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Debug)
# set(CMAKE_CXX_CLANG_TIDY clang-tidy)

include(cmake/prelude.cmake)

project(
  kvstore
  VERSION 1.0.0
  DESCRIPTION "A key-value store implementation"
  HOMEPAGE_URL "https://github.com/kaspar-p/kvstore"
  LANGUAGES CXX
)

include(cmake/project-is-top-level.cmake)
include(cmake/variables.cmake)

# Declare third-party dependencies
add_subdirectory(third_party/xxHash/cmake_unofficial third_party/xxHash/build EXCLUDE_FROM_ALL)

# ---- Declare testing directory ----
add_subdirectory(tests)

# ---- Declare libraries ----

# naming.cpp
add_library(kvstore_naming OBJECT src/naming.cpp)
target_include_directories(
  kvstore_naming ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_naming PUBLIC cxx_std_17)
target_link_libraries(kvstore_naming PRIVATE xxHash::xxhash)

# fileutil.cpp
add_library(kvstore_file OBJECT src/fileutil.cpp)
target_include_directories(
  kvstore_file ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_file PUBLIC cxx_std_17)

# dbg.cpp
add_library(kvstore_dbg OBJECT src/dbg.cpp)
target_include_directories(
  kvstore_dbg ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_dbg PUBLIC cxx_std_17)

# memtable.cpp
add_library(kvstore_memtable OBJECT src/memtable.cpp)
target_include_directories(
  kvstore_memtable ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_memtable PUBLIC cxx_std_17)

# manifest.cpp
add_library(kvstore_manifest OBJECT src/manifest.cpp)
target_include_directories(
  kvstore_manifest ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_manifest PUBLIC cxx_std_17)

# sstable_naive.cpp + sstable_btree.cpp
add_library(kvstore_sstable OBJECT src/sstable_naive.cpp src/sstable_btree.cpp)
target_include_directories(
  kvstore_sstable ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_sstable PUBLIC cxx_std_17)
target_link_libraries(kvstore_sstable PRIVATE xxHash::xxhash)

# evict.cpp
add_library(kvstore_evict OBJECT src/evict.cpp)
target_include_directories(
  kvstore_evict ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_evict PUBLIC cxx_std_17)
target_link_libraries(kvstore_evict PRIVATE xxHash::xxhash)

# lsm.cpp
add_library(kvstore_lsm OBJECT src/lsm.cpp)
target_include_directories(
  kvstore_lsm ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_lsm PUBLIC cxx_std_17)
target_link_libraries(kvstore_lsm PRIVATE xxHash::xxhash)

# buf.cpp
add_library(kvstore_buf OBJECT src/buf.cpp)
target_include_directories(
  kvstore_buf ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_buf PUBLIC cxx_std_17)
target_link_libraries(kvstore_buf PRIVATE xxHash::xxhash)

# filter.cpp
add_library(kvstore_filter OBJECT src/filter.cpp)
target_include_directories(
  kvstore_filter ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_filter PUBLIC cxx_std_17)
target_link_libraries(kvstore_filter PRIVATE xxHash::xxhash)
target_link_libraries(kvstore_filter PRIVATE kvstore_buf)

# kvstore.cpp
add_library(kvstore_kvstore OBJECT src/kvstore.cpp)
target_include_directories(
  kvstore_kvstore ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_kvstore PUBLIC cxx_std_17)
target_link_libraries(kvstore_kvstore PRIVATE xxHash::xxhash)

# benchmarks.cpp
add_library(kvstore_benchmarks OBJECT src/benchmarks.cpp)
target_include_directories(
  kvstore_benchmarks ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_benchmarks PUBLIC cxx_std_17)


# ---- Declare executables ----
add_executable(kvstore_exe src/main.cpp)
add_executable(kvstore::exe ALIAS kvstore_exe)
add_executable(benchmarks_exe src/benchmarks.cpp)
add_executable(benchmarks::exe ALIAS benchmarks_exe)

set_property(TARGET kvstore_exe PROPERTY OUTPUT_NAME kvstore)
set_property(TARGET benchmarks_exe PROPERTY OUTPUT_NAME benchmarks)

target_compile_features(kvstore_exe PRIVATE cxx_std_17)
target_compile_features(benchmarks_exe PRIVATE cxx_std_17)

# ---- Link external libraries ----
target_link_libraries(kvstore_exe PRIVATE xxHash::xxhash)
target_link_libraries(benchmarks_exe PRIVATE xxHash::xxhash)

# ---- Link internal libraries ----
target_link_libraries(kvstore_exe PRIVATE kvstore_kvstore)
target_link_libraries(kvstore_exe PRIVATE kvstore_sstable)
target_link_libraries(kvstore_exe PRIVATE kvstore_buf)
target_link_libraries(kvstore_exe PRIVATE kvstore_evict)
target_link_libraries(kvstore_exe PRIVATE kvstore_dbg)
target_link_libraries(kvstore_exe PRIVATE kvstore_memtable)
target_link_libraries(kvstore_exe PRIVATE kvstore_filter)
target_link_libraries(kvstore_exe PRIVATE kvstore_lsm)
target_link_libraries(kvstore_exe PRIVATE kvstore_file)
target_link_libraries(kvstore_exe PRIVATE kvstore_naming)
target_link_libraries(kvstore_exe PRIVATE kvstore_manifest)

target_link_libraries(benchmarks_exe PRIVATE kvstore_kvstore)
target_link_libraries(benchmarks_exe PRIVATE kvstore_sstable)
target_link_libraries(benchmarks_exe PRIVATE kvstore_buf)
target_link_libraries(benchmarks_exe PRIVATE kvstore_evict)
target_link_libraries(benchmarks_exe PRIVATE kvstore_dbg)
target_link_libraries(benchmarks_exe PRIVATE kvstore_memtable)
target_link_libraries(benchmarks_exe PRIVATE kvstore_filter)
target_link_libraries(benchmarks_exe PRIVATE kvstore_lsm)
target_link_libraries(benchmarks_exe PRIVATE kvstore_file)
target_link_libraries(benchmarks_exe PRIVATE kvstore_naming)
target_link_libraries(benchmarks_exe PRIVATE kvstore_manifest)

# ---- Install rules ----
if(NOT CMAKE_SKIP_INSTALL_RULES)
  include(cmake/install-rules.cmake)
endif()

# ---- Developer mode ----
if(NOT kvstore_DEVELOPER_MODE)
  return()
elseif(NOT PROJECT_IS_TOP_LEVEL)
  message(
    AUTHOR_WARNING
    "Developer mode is intended for developers of kvstore"
  )
endif()

include(cmake/dev-mode.cmake)
