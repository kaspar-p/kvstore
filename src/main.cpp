#include <cassert>
#include <exception>
#include <filesystem>
#include <iostream>

#include "dbg.hpp"
#include "filter.hpp"
#include "kvstore.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "xxhash.h"

DbNaming create_dir(std::string name) {
  bool exists = std::filesystem::exists("/tmp/" + name);
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/" + name);
    assert(created);
  }
  return DbNaming{.dirpath = "/tmp/" + name, .name = name};
}

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16});
}
BufPool buf = test_buffer();

std::vector<K> test_keys(int num) {
  std::vector<K> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(i);
  }
  return keys;
}

int main() {
  std::filesystem::remove_all("./Main.DataTesting");

  KvStore table;
  table.Open("Main.DataTesting", Options{.dir = "./"});

  for (int i = 0; i < 1000 * 1000; i++) {
    table.Put(i, 2 * i);
  }
}
