cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Debug)

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

# ---- Declare testing directory ----
add_subdirectory(tests)

# ---- Declare libraries ----

# memtable.cpp
add_library(kvstore_memtable OBJECT src/memtable.cpp)
target_include_directories(
  kvstore_memtable ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_memtable PUBLIC cxx_std_17)

# sstable_naive.cpp + sstable_btree.cpp
add_library(kvstore_sstable OBJECT src/sstable_naive.cpp src/sstable_btree.cpp)
target_include_directories(
  kvstore_sstable ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_sstable PUBLIC cxx_std_17)

# buf.cpp
add_library(kvstore_buf OBJECT src/buf.cpp)
target_include_directories(
  kvstore_buf ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_buf PUBLIC cxx_std_17)

# kvstore.cpp
add_library(kvstore_kvstore OBJECT src/kvstore.cpp)
target_include_directories(
  kvstore_kvstore ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_compile_features(kvstore_kvstore PUBLIC cxx_std_17)

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

# ---- Link libraries ----
target_link_libraries(kvstore_exe PRIVATE kvstore_kvstore)
target_link_libraries(kvstore_exe PRIVATE kvstore_sstable)
target_link_libraries(kvstore_exe PRIVATE kvstore_buf)
target_link_libraries(kvstore_exe PRIVATE kvstore_memtable)
target_link_libraries(benchmarks_exe PRIVATE kvstore_kvstore)
target_link_libraries(benchmarks_exe PRIVATE kvstore_sstable)
target_link_libraries(benchmarks_exe PRIVATE kvstore_buf)
target_link_libraries(benchmarks_exe PRIVATE kvstore_memtable)

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
