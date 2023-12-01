#include <chrono>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

auto benchmark_scan(KvStore& db, uint64_t nodes_to_scan) {
  for (K i = 0; i < nodes_to_scan; i++) {
    db.Put(i, i);
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  std::vector<std::pair<K, V>> result = db.Scan(0, nodes_to_scan);
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
  return ms_int;
}

int main() {
  uint64_t max_size = 8;  // TODO use 1024
  Options options = Options{
      .dir = "/tmp",
  };
  std::vector<std::string> results = run_multiple(
      1, max_size, benchmark_scan, "Benchmarks.DataVolumeVsScan", options);
  write_to_csv("data_volume_vs_scan.csv",
               "inputDataSize (MB),scan times,scan throughput", results);
}
