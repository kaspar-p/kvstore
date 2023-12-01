#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "constants.hpp"
#include "kvstore.hpp"

double calculate_throughput(uint64_t megabytes,
                            std::chrono::microseconds time) {
  double time_in_seconds = static_cast<double>(time.count()) / 1000000.0;
  return static_cast<double>(megabytes) / time_in_seconds;
}

void write_to_csv(std::string file_name, std::string header,
                  std::vector<std::string> results) {
  std::cout << "Writing benchmarks to " << file_name << '\n';
  std::ofstream file(file_name);
  file << header << "\n";
  for (auto& result : results) {
    file << result << "\n";
  }
  file.close();
}

std::string run_experiment(
    KvStore& db, uint64_t data_size_in_mb,
    std::function<std::chrono::microseconds(KvStore&, uint64_t)>
        benchmark_function) {
  uint64_t data_size_in_nodes = data_size_in_mb * kMegabyteSize / kKeySize;

  std::chrono::microseconds benchmark_time =
      benchmark_function(db, data_size_in_nodes);
  auto benchmark_time_throughput =
      calculate_throughput(data_size_in_mb, benchmark_time);

  std::string result = std::to_string(data_size_in_mb) + "," +
                       std::to_string(benchmark_time.count()) + "," +
                       std::to_string(benchmark_time_throughput);
  return result;
}

std::vector<std::string> run_multiple(
    uint64_t min_data_size_mb, uint64_t max_data_size_mb,
    const std::function<std::chrono::microseconds(KvStore&, uint64_t)>&
        benchmark_function,
    const std::string& root_directory) {
  std::vector<std::string> results;
  uint64_t data_size = min_data_size_mb;
  while (data_size <= max_data_size_mb) {
    std::cout << "Running experiment for " << data_size << "MB\n";
    KvStore db;
    Options options = Options{
        .dir = "/tmp",
    };
    std::string db_dir(root_directory + "_" + std::to_string(data_size) +
                       ".db");
    std::filesystem::remove_all("/tmp/" + db_dir);
    db.Open(db_dir, options);
    results.push_back(run_experiment(db, data_size, benchmark_function));
    data_size *= 2;
    db.Close();
  }
  return results;
}