#include <chrono>
#include <string>
#include <vector>

#include "experiments.hpp"
#include "kvstore.hpp"

std::chrono::microseconds benchmark_inserts(KvStore& db,
                                            uint64_t nodes_to_insert) {
  auto t1 = std::chrono::high_resolution_clock::now();
  for (K i = 0; i < nodes_to_insert; i++) {
    db.Put(i, i);
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
  std::vector<std::string> results = run_multiple(
      1, max_size, benchmark_inserts, "Benchmarks.DataVolumeVsPut", options);
  write_to_csv("data_volume_vs_put.csv",
               "inputDataSize (MB),insert time,insert throughput", results);
}
