# Run a release script. Runs unit tests, generates benchmarks (someday), and checks for memory leaks.

if [[ "$(basename $(pwd))" != "kvstore" ]]; then
  echo "Please run this command from project root!"
  exit 1
fi

# Build
rm -rf build
cmake -S . -B build
cmake --build build

# Test
./build/tests/kvstore_test

# Memory Leaks
if [[ $(uname) == "Linux" ]]; then # Condition works on Kaspar's EC2 instance, might not work for everyone
  valgrind ./build/tests/kvstore_test
else
  echo "Not on a Linux machine, skipping valgrind memleak tests"
fi

