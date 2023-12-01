#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

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
    (void)value;
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

int main() {
  uint64_t max_size_mb = 4;  // TODO use 1024

  Options btree_options = {.dir = "/tmp",
                           .serialization = DataFileFormat::kBTree};

  Options sorted_options = {.dir = "/tmp",
                            .serialization = DataFileFormat::kFlatSorted};

  std::vector<
      std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>>
      benchmark_functions;
  benchmark_functions.emplace_back(benchmark_get_random);

  std::vector<std::vector<std::string>> btree_results =
      run_with_increasing_data_size(max_size_mb, benchmark_functions,
                                    "Benchmarks.Btree", btree_options);

  std::vector<std::vector<std::string>> sorted_results =
      run_with_increasing_data_size(max_size_mb, benchmark_functions,
                                    "Benchmarks.Sorted", btree_options);

  write_to_csv("btree_queries.csv", "inputDataSize (MB),throughput",
               btree_results[0]);

  write_to_csv("sorted_queries.csv", "inputDataSize (MB),throughput",
               sorted_results[0]);
}
