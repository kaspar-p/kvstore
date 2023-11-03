#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <fstream>

#include "constants.hpp"
#include "kvstore.hpp"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using BenchmarkResult = std::tuple<uint64_t, std::chrono::microseconds, double, std::chrono::microseconds, double, std::chrono::microseconds, double, std::chrono::microseconds, double>;


auto benchmark_inserts(KvStore& db, uint64_t nodes_to_insert)
{
  auto t1 = high_resolution_clock::now();
  for (K i = 0; i < nodes_to_insert; i++) {
    db.Put(i, i);
  }
  auto t2 = high_resolution_clock::now();
  auto ms_int = duration_cast<microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_scan(KvStore& db, K lower, K upper)
{
  auto t1 = high_resolution_clock::now();
  std::vector<std::pair<K, V>> result = db.Scan(lower, upper);
  auto t2 = high_resolution_clock::now();
  auto ms_int = duration_cast<microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_get_sequential(KvStore& db, K lower, K upper)
{
  auto t1 = high_resolution_clock::now();
  for (K i = lower; i < upper; i++) {
    const std::optional<V> v = db.Get(i);
  }
  auto t2 = high_resolution_clock::now();
  auto ms_int = duration_cast<microseconds>(t2 - t1);
  return ms_int;
}

auto benchmark_get_random(KvStore& db, uint64_t nodes_to_get, K lower, K upper)
{
  std::random_device r;
  std::mt19937 eng(r());
  std::uniform_int_distribution<K> dist(lower, upper);
  std::vector<K> random_keys(nodes_to_get);

  for (int i = 0; i < nodes_to_get; i++) {
    random_keys[i] = static_cast<K>(dist(eng));
  }

  auto t1 = high_resolution_clock::now();
  for (K key : random_keys) {
    const std::optional<V> value = db.Get(key);
  }
  auto t2 = high_resolution_clock::now();
  auto ms_int = duration_cast<microseconds>(t2 - t1);
  return ms_int;
}

void write_to_csv(const std::string& file_name, std::vector<BenchmarkResult> &results)
{
  std::cout << "Writing benchmarks to " << file_name << std::endl;
  std::ofstream file(file_name);
  file << "inputDataSize (MB),insert time,insert throughput,scan time,scan throughput,sequential get time,sequential get throughput,random get time,random get throughput\n";
    for (const BenchmarkResult& result : results) {
    file << std::get<0>(result) << "," << std::get<1>(result).count() << ","
              << std::get<2>(result) << "," << std::get<3>(result).count() << ","
              << std::get<4>(result) << "," << std::get<5>(result).count() << "," 
              << std::get<6>(result) << "," << std::get<7>(result).count() << "," 
              << std::get<8>(result) << std::endl;
  }
  file.close();
}

double calculate_throughput(uint64_t megabytes, std::chrono::microseconds time)
{
  double time_in_seconds = static_cast<double>(time.count()) / 1000000.0;
  return static_cast<double>(megabytes) / time_in_seconds;
}

void run_benchmarks(uint64_t megabytes_to_insert, std::vector<BenchmarkResult> &results)
{
  std::cout << "Running benchmarks for " << megabytes_to_insert << "MB" << std::endl;
  uint64_t nodes_to_insert = megabytes_to_insert * kMegabyteSize / kKeySize;
  KvStore db {};
  Options options;
  options.overwrite = true;
  std::string db_dir("benchmark_test_" + std::to_string(megabytes_to_insert) + ".db");
  db.Open(db_dir, options);

  std::chrono::microseconds insert_time = benchmark_inserts(db, nodes_to_insert);
  auto insert_throughput = calculate_throughput(megabytes_to_insert, insert_time);
  
  std::chrono::microseconds full_scan_time =
      benchmark_scan(db, static_cast<K>(0), static_cast<K>(nodes_to_insert));
  auto full_scan_throughput = calculate_throughput(megabytes_to_insert, full_scan_time);

  std::chrono::microseconds sequential_get_time = benchmark_get_sequential(
      db, static_cast<K>(0), static_cast<K>(nodes_to_insert));
  auto sequential_get_throughput = calculate_throughput(megabytes_to_insert, sequential_get_time);

  std::chrono::microseconds random_get_time = benchmark_get_random(
      db, nodes_to_insert, static_cast<K>(0), static_cast<K>(nodes_to_insert));
  auto random_get_throughput = calculate_throughput(megabytes_to_insert, random_get_time);

  BenchmarkResult result = std::make_tuple(
      megabytes_to_insert,
      insert_time,
      insert_throughput,
      full_scan_time,
      full_scan_throughput,
      sequential_get_time,
      sequential_get_throughput,
      random_get_time,
      random_get_throughput
  );
  results.push_back(result);
  
  db.Close();
}

int main()
{
  uint64_t megabytes_to_insert = 1;
  uint64_t max_size = 128;
  std::vector<BenchmarkResult> results;

  try {
    while (megabytes_to_insert <= max_size) {
      run_benchmarks(megabytes_to_insert, results);
      megabytes_to_insert *= 2;
    }

    write_to_csv("benchmarks.csv", results);

  } catch (std::exception& e) {
    std::cerr << "Got exception " << e.what() << '\n';
  }
}
