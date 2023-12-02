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
  if (!std::filesystem::exists("experiment_csvs")) {
    std::filesystem::create_directory("experiment_csvs");
  }
  std::cout << "Writing benchmarks to experiment_csvs/" << file_name << '\n';
  std::ofstream file("experiment_csvs/" + file_name);
  file << header << "\n";
  for (auto& result : results) {
    file << result << "\n";
  }
  file.close();
}

std::string run_experiment(
    KvStore& db, uint64_t lower, uint64_t upper,
    std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>
        benchmark_function) {
  uint64_t lower_nodes = lower * kMegabyteSize / sizeof(std::pair<K, V>);
  uint64_t upper_nodes = upper * kMegabyteSize / sizeof(std::pair<K, V>);

  std::chrono::microseconds benchmark_time =
      benchmark_function(db, lower_nodes, upper_nodes);
  auto benchmark_time_throughput =
      calculate_throughput(upper - lower, benchmark_time);

  std::string result =
      std::to_string(upper) + "," + std::to_string(benchmark_time_throughput);
  return result;
}

std::vector<std::vector<std::string>> run_with_increasing_data_size(
    uint64_t max_data_size_mb,
    std::vector<
        std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>>&
        benchmark_functions,
    const std::string& root_directory, Options options) {
  uint64_t data_size = 1;

  KvStore db;
  std::string db_dir(root_directory + "_" + std::to_string(data_size) + ".db");
  std::filesystem::remove_all("/tmp/" + db_dir);
  db.Open(db_dir, options);

  std::vector<std::vector<std::string>> results;
  for (auto benchmark_function : benchmark_functions) {
    std::vector<std::string> single_result;
    results.push_back(single_result);
  }

  while (data_size <= max_data_size_mb) {
    std::cout << "Running experiment for " << data_size / 2 << " to "
              << data_size << "MB\n";
    for (int i = 0; i < benchmark_functions.size(); i++) {
      results[i].push_back(
          run_experiment(db, data_size / 2, data_size, benchmark_functions[i]));
    }
    data_size *= 2;
  }
  return results;
}