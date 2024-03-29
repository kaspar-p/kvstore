# Parent project does not export its library target, so this CML implicitly
# depends on being added from it, i.e. the testing is done only from the build
# tree and is not feasible from an install location
cmake_minimum_required(VERSION 3.14)

project(kvstoreTests LANGUAGES CXX)

set(CMAKE_CXX_FLAGS_DEBUG "-g")

# ---- Dependencies ----
include(FetchContent)
FetchContent_Declare(
  googletest
  # Specify the commit you depend on and update it regularly.
  URL https://github.com/google/googletest/archive/5376968f6948923e2411081fd9372e71a59d8e77.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# ---- Testing Libraries ----
# testutil.cpp
add_library(kvstore_testutil OBJECT src/testutil.cpp)
target_include_directories(
  kvstore_testutil ${warning_guard}
  PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_link_libraries(kvstore_testutil PRIVATE xxHash::xxhash)
target_link_libraries(kvstore_testutil PRIVATE kvstore_naming)
target_link_libraries(kvstore_testutil PRIVATE gtest_main)
target_compile_features(kvstore_testutil PUBLIC cxx_std_17)

# ---- Kvstore Tests ----
add_executable(kvstore_test 
  src/kvstore.test.cpp
  src/buf.test.cpp
  src/memtable.test.cpp
  src/sstable_naive.test.cpp
  src/sstable_btree.test.cpp
  src/evict.test.cpp
  src/filter.test.cpp
  src/dbg.test.cpp
  src/fileutil.test.cpp
  src/manifest.test.cpp
  src/naming.test.cpp
  src/minheap.test.cpp
  src/lsm.test.cpp
)

target_link_libraries(kvstore_test PRIVATE kvstore_naming)
target_link_libraries(kvstore_test PRIVATE kvstore_testutil)
target_link_libraries(kvstore_test PRIVATE kvstore_manifest)
target_link_libraries(kvstore_test PRIVATE kvstore_file)
target_link_libraries(kvstore_test PRIVATE kvstore_filter)
target_link_libraries(kvstore_test PRIVATE kvstore_dbg)
target_link_libraries(kvstore_test PRIVATE kvstore_memtable)
target_link_libraries(kvstore_test PRIVATE kvstore_buf)
target_link_libraries(kvstore_test PRIVATE kvstore_evict)
target_link_libraries(kvstore_test PRIVATE kvstore_minheap)
target_link_libraries(kvstore_test PRIVATE kvstore_lsm)
target_link_libraries(kvstore_test PRIVATE kvstore_sstable)
target_link_libraries(kvstore_test PRIVATE kvstore_kvstore)
target_link_libraries(kvstore_test PRIVATE gtest_main)
target_link_libraries(kvstore_test PRIVATE xxHash::xxhash)
target_compile_features(kvstore_test PRIVATE cxx_std_17)
add_test(NAME kvstore_test COMMAND kvstore_test)

add_test(NAME memtable_test COMMAND memtable_test)

# ---- End-of-file commands ----