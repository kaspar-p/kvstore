#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

auto benchmark_get_random(KvStore& db, uint64_t nodes_to_get, K lower,
                          K upper) {
  std::random_device r;
  std::mt19937 eng(r());
  std::uniform_int_distribution<K> dist(lower, upper);
  std::vector<K> random_keys(nodes_to_get);

  for (std::size_t i = 0; i < nodes_to_get; i++) {
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

std::chrono::microseconds benchmark_btree(KvStore& db,
                                          uint64_t nodes_to_insert) {
  for (K i = 0; i < nodes_to_insert; i++) {
    db.Put(i, i);
  }
  return benchmark_get_random(db, nodes_to_insert, static_cast<K>(0),
                              static_cast<K>(nodes_to_insert));
}

int main() {
  uint64_t max_size = 4;  // TODO use 1024

  Options btree_options = {.dir = "/tmp",
                           .serialization = DataFileFormat::kBTree};

  Options sorted_options = {.dir = "/tmp",
                            .serialization = DataFileFormat::kFlatSorted};

  std::vector<std::string> btree_results = run_multiple(
      1, max_size, benchmark_btree, "Benchmarks.BtreeGetRandom", btree_options);

  write_to_csv("btree_random_get.csv",
               "inputDataSize (MB),random get time,random get throughput",
               btree_results);

  std::vector<std::string> sorted_results =
      run_multiple(1, max_size, benchmark_btree, "Benchmarks.SortedGetRandom",
                   sorted_options);

  write_to_csv("sorted_random_get.csv",
               "inputDataSize (MB),random get time,random get throughput",
               sorted_results);
}
