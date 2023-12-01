#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "kvstore.hpp"

double calculate_throughput(uint64_t megabytes, std::chrono::microseconds time);

void write_to_csv(std::string file_name, std::string header,
                  std::vector<std::string> results);

std::string run_experiment(
    KvStore& db, uint64_t lower, uint64_t upper,
    std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>
        benchmark_function);

std::vector<std::vector<std::string>> run_with_increasing_data_size(
    uint64_t max_data_size_mb,
    std::vector<
        std::function<std::chrono::microseconds(KvStore&, uint64_t, uint64_t)>>&
        benchmark_functions,
    const std::string& root_directory, Options options);
