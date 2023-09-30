# Run a release script. Runs unit tests, generates benchmarks (someday), and checks for memory leaks.

if [[ "$(basename)" != "kvstore" ]]; then
  echo "Please run this command from project root!"
  exit 1
fi

# Build
rm -rf build
cmake -S . -B build
cmake --build build

# Test
./build/tests/kvstore_test

# TODO: benchmarks, valgrind tests
