#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

auto benchmark_get_random(KvStore& db, uint64_t lower, uint64_t upper,
                          uint64_t operations) {
  for (K i = lower; i < upper; i++) {
    db.Put(i, i);
  }

  std::random_device r;
  std::mt19937 eng(r());
  std::uniform_int_distribution<K> dist(0, upper);
  std::vector<K> random_keys(operations);

  for (std::size_t i = 0; i < operations; i++) {
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

  Options binary_search_options = {
      .dir = "/tmp", .serialization = DataFileFormat::kFlatSorted};

  std::vector<std::function<std::chrono::microseconds(KvStore&, uint64_t,
                                                      uint64_t, uint64_t)>>
      benchmark_functions;
  benchmark_functions.emplace_back(benchmark_get_random);

  uint64_t operations = kMegabyteSize / sizeof(std::pair<K, V>);

  std::vector<std::vector<std::string>> btree_results =
      run_with_increasing_data_size(max_size_mb, benchmark_functions,
                                    "Benchmarks.Btree", btree_options,
                                    operations);

  std::vector<std::vector<std::string>> sorted_results =
      run_with_increasing_data_size(max_size_mb, benchmark_functions,
                                    "Benchmarks.BinarySearch",
                                    binary_search_options, operations);

  write_to_csv("stage_2_btree_search.csv", "inputDataSize (MB),throughput",
               btree_results[0]);

  write_to_csv("stage_2_binary_search.csv", "inputDataSize (MB),throughput",
               sorted_results[0]);
}
