#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "filter.hpp"
#include "kvstore.hpp"
#include "memtable.hpp"
#include "naming.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  std::filesystem::remove_all("/tmp/KvStore.ScanAcrossRunsSimple");

  KvStore table;
  Options opts = Options{
      .dir = "/tmp",
      .memory_buffer_elements = 2,
  };
  table.Open("KvStore.ScanAcrossRunsSimple", opts);

  std::vector<uint64_t> vals{};
  for (uint64_t i = 10; i < 90; i++) {
    vals.push_back(i);
  }

  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(vals.begin(), vals.end(), g);

  for (const auto& v : vals) {
    std::cout << v << '\n';
  }

  // Add to the table
  for (const auto& i : vals) {
    table.Put(i, 2 * i);
  }

  // Centered
  auto v = table.Scan(20, 60);

  for (int i = 0; i < v.size(); i++) {
    std::cout << "FOUND: " << i << " === " << v.at(i).first << ","
              << v.at(i).second << '\n';
  }

  assert(v.size() == 41);
  for (std::size_t i = 0; i < v.size(); i++) {
    std::cout << "testing " << i << " === " << v.at(i).first << ","
              << v.at(i).second << '\n';
    assert(v.at(i).first == i + 20);
    assert(v.at(i).second == 2 * (i + 20));
  }

  // Left hanging
  v = table.Scan(0, 20);
  assert(v.size() == 11);
  for (std::size_t i = 0; i < v.size(); i++) {
    assert(v.at(i).first == i + 10);
    assert(v.at(i).second == 2 * (i + 10));
  }

  // Right hanging
  v = table.Scan(50, 150);
  assert(v.size() == 40);
  for (std::size_t i = 0; i < v.size(); i++) {
    assert(v.at(i).first == i + 50);
    assert(v.at(i).second == 2 * (i + 50));
  }

  // Both hanging
  v = table.Scan(0, 150);
  assert(v.size() == 80);
  for (std::size_t i = 0; i < v.size(); i++) {
    assert(v.at(i).first == i + 10);
    assert(v.at(i).second == 2 * (i + 10));
  }
}