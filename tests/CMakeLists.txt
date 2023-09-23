# Parent project does not export its library target, so this CML implicitly
# depends on being added from it, i.e. the testing is done only from the build
# tree and is not feasible from an install location
cmake_minimum_required(VERSION 3.14)

project(kvstoreTests LANGUAGES CXX)

set(CMAKE_CXX_FLAGS_DEBUG "-g")

# ---- Dependencies ----
find_package(GTest REQUIRED)
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
include(GoogleTest)

enable_testing()

# ---- Tests ----
add_executable(kvstore_test src/kvstore_test.cpp)
target_link_libraries(kvstore_test PRIVATE kvstore_memtable)
target_link_libraries(kvstore_test PRIVATE GTest::gtest_main)
target_compile_features(kvstore_test PRIVATE cxx_std_17)

gtest_discover_tests(kvstore_test)

add_test(NAME kvstore_test COMMAND kvstore_test)

# ---- End-of-file commands ----