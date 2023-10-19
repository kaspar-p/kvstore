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

# ---- Kvstore Tests ----
add_executable(kvstore_test src/kvstore_test.cpp)
target_link_libraries(kvstore_test PRIVATE kvstore_memtable)
target_link_libraries(kvstore_test PRIVATE kvstore_sstable)
target_link_libraries(kvstore_test PRIVATE kvstore_kvstore)
target_link_libraries(kvstore_test PRIVATE gtest_main)
target_compile_features(kvstore_test PRIVATE cxx_std_17)
add_test(NAME kvstore_test COMMAND kvstore_test)

# ---- SstableNaive Tests ----
add_executable(sstable_naive_test src/sstable_naive.test.cpp)
target_link_libraries(sstable_naive_test PRIVATE kvstore_memtable)
target_link_libraries(sstable_naive_test PRIVATE kvstore_sstable)
target_link_libraries(sstable_naive_test PRIVATE gtest_main)
target_compile_features(sstable_naive_test PRIVATE cxx_std_17)

add_test(NAME sstable_naive_test COMMAND sstable_naive_test)

# ---- Memtable Tests ----
add_executable(memtable_test src/memtable.test.cpp)
target_link_libraries(memtable_test PRIVATE kvstore_memtable)
target_link_libraries(memtable_test PRIVATE gtest_main)
target_compile_features(memtable_test PRIVATE cxx_std_17)

add_test(NAME memtable_test COMMAND memtable_test)

# ---- End-of-file commands ----