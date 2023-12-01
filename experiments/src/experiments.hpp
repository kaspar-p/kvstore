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
    KvStore& db, uint64_t data_size_in_mb,
    std::function<std::chrono::microseconds(KvStore&, uint64_t)>
        benchmark_function);

std::vector<std::string> run_multiple(
    uint64_t min_data_size_mb, uint64_t max_data_size_mb,
    const std::function<std::chrono::microseconds(KvStore&, uint64_t)>&
        benchmark_function,
    const std::string& root_directory, Options options);
