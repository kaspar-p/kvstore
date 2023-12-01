#include <chrono>
#include <random>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

auto benchmark_get_sequential(KvStore& db, uint64_t nodes_to_get) {
  for (K i = 0; i < nodes_to_get; i++) {
    db.Put(i, i);
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  for (K i = 0; i < nodes_to_get; i++) {
    const std::optional<V> v = db.Get(i);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_get_random(KvStore& db, uint64_t nodes_to_get) {
  for (K i = 0; i < nodes_to_get; i++) {
    db.Put(i, i);
  }

  std::random_device r;
  std::mt19937 eng(r());
  std::uniform_int_distribution<K> dist(0, nodes_to_get);
  std::vector<K> random_keys(nodes_to_get);

  for (std::size_t i = 0; i < nodes_to_get; i++) {
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

int main() {
  uint64_t max_size = 8;  // TODO use 1024
  Options options = Options{
      .dir = "/tmp",
  };
  std::vector<std::string> sequential_results =
      run_multiple(1, max_size, benchmark_get_sequential,
                   "Benchmarks.DataVolumeVsGetSequential", options);
  write_to_csv("data_volume_vs_get_sequential.csv",
               "inputDataSize (MB),get time,get throughput",
               sequential_results);

  std::vector<std::string> random_results =
      run_multiple(1, max_size, benchmark_get_random,
                   "Benchmarks.DataVolumeVsGetRandom", options);
  write_to_csv("data_volume_vs_get_random.csv",
               "inputDataSize (MB),get time,get throughput", random_results);
}
