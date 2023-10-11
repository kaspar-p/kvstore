#include <cstdlib>
#include <iostream>
#include <chrono>
#include <optional>
#include <random>

#include "kvstore.hpp"
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
    
auto benchmark_inserts(KvStore& db, uint64_t nodes_to_insert) {
    auto t1 = high_resolution_clock::now();
    for (K i = 0; i < nodes_to_insert; i++) {
        db.Put(i, V(i));
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<microseconds>(t2 - t1);
    return ms_int;
}

auto benchmark_scan(KvStore& db, K lower, K upper) {
    auto t1 = high_resolution_clock::now();
    std::vector<std::pair<K, V>> result = db.Scan(lower, upper);
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<microseconds>(t2 - t1);
    return ms_int;
}

auto benchmark_get_sequential(KvStore& db, K lower, K upper) {
    auto t1 = high_resolution_clock::now();
    for (K i = lower; i < upper; i++) {
        const std::optional<V> v = db.Get(i);
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<microseconds>(t2 - t1);
    return ms_int;
}

auto benchmark_get_random(KvStore& db, uint64_t nodes_to_get, K lower, K upper) {
    std::random_device r;
    std::mt19937 eng(r());
    std::uniform_int_distribution<K> dist(lower, upper);
    std::vector<K> random_keys(nodes_to_get);
    
    for (int i = 0; i < nodes_to_get; i++) {
        random_keys[i] = K(dist(eng));
    }

    auto t1 = high_resolution_clock::now();
    for (K key : random_keys) {
        const std::optional<V> value = db.Get(key);
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<microseconds>(t2 - t1);
    return ms_int;
}

void run_benchmarks(uint64_t nodes_to_insert) {
    std::cout << "Running benchmarks for " << nodes_to_insert << " nodes\n";
    KvStore db {};
    Options options;
    options.overwrite = true;
    db.Open("benchmark_test.db", options);
    
    auto insert_time = benchmark_inserts(db, nodes_to_insert);

    auto full_scan_time = benchmark_scan(db, K(0), K(nodes_to_insert));

    auto sequential_get_time = benchmark_get_sequential(db, K(0), K(nodes_to_insert));

    auto random_get_time = benchmark_get_random(db, nodes_to_insert, K(0), K(nodes_to_insert));

      std::cout << "INSERT TIME: " << insert_time.count() << "\n"
            << "FULL SCAN TIME: " << full_scan_time.count() << "\n"
            << "SEQUENTIAL GET TIME: " << sequential_get_time.count() << "\n"
            << "RANDOM GET TIME: " << random_get_time.count() << std::endl;
}


int main(int argc, char* argv[])
{
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <nodes_to_insert>" << std::endl;
        return 1;
    }

    uint64_t nodes_to_insert = std::stoull(argv[1]);

    run_benchmarks(nodes_to_insert);
}
