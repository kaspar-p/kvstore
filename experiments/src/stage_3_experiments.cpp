#include <chrono>
#include <random>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

std::chrono::microseconds benchmark_put(KvStore& db, uint64_t lower,
                                        uint64_t upper) {
  auto t1 = std::chrono::high_resolution_clock::now();
  for (K i = lower; i < upper; i++) {
    db.Put(i, i);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_get_sequential(KvStore& db, uint64_t lower, uint64_t upper) {
  auto t1 = std::chrono::high_resolution_clock::now();
  for (K i = lower; i < upper; i++) {
    const std::optional<V> v = db.Get(i);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_get_random(KvStore& db, uint64_t lower, uint64_t upper) {
  std::random_device r;
  std::mt19937 eng(r());
  std::uniform_int_distribution<K> dist(lower, upper);
  std::vector<K> random_keys(upper - lower);

  for (std::size_t i = 0; i < upper - lower; i++) {
    random_keys[i] = static_cast<K>(dist(eng));
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  for (K key : random_keys) {
    const std::optional<V> value = db.Get(key);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_scan(KvStore& db, uint64_t lower, uint64_t upper) {
  auto t1 = std::chrono::high_resolution_clock::now();
  std::vector<std::pair<K, V>> result = db.Scan(lower, upper);
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

int main() {
  uint64_t max_size_mb = 8;  // TODO use 1024
  Options options = Options{
      .dir = "/tmp",
      .memory_buffer_elements = kMegabyteSize / sizeof(std::pair<K, V>),
      .buffer_pages_maximum = kMegabyteSize * 10 / kPageSize,
  };

  std::vector<
      std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>>
      benchmark_functions;
  benchmark_functions.emplace_back(benchmark_put);
  benchmark_functions.emplace_back(benchmark_get_random);
  benchmark_functions.emplace_back(benchmark_get_sequential);
  benchmark_functions.emplace_back(benchmark_scan);

  std::vector<std::vector<std::string>> results = run_with_increasing_data_size(
      max_size_mb, benchmark_functions, "Benchmarks.Stage3", options);

  write_to_csv("stage_3_put.csv", "inputDataSize (MB),throughput", results[0]);

  write_to_csv("stage_3_get_random.csv", "inputDataSize (MB),throughput",
               results[1]);

  write_to_csv("stage_3_get_sequential.csv", "inputDataSize (MB),throughput",
               results[2]);

  write_to_csv("stage_3_scan.csv", "inputDataSize (MB),throughput", results[3]);
}
