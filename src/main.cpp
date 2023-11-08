#include <cassert>
#include <exception>
#include <filesystem>
#include <iostream>

#include "dbg.hpp"
#include "filter.hpp"
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

std::vector<K> test_keys(int num) {
  std::vector<K> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(i);
  }
  return keys;
}

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16});
}
BufPool buf = test_buffer();

int main() {
  auto name = create_dir("Filter.Recovery");
  {
    std::filesystem::remove(filter_file(name, 1));

    Filter f(name, 1, buf, 0, test_keys(128));
    assert(f.Has(999) == false);
    assert(std::filesystem::file_size(filter_file(name, 1)) % kPageSize == 0);

    for (auto &key : test_keys(128)) {
      std::cout << "testing " << key << std::endl;
      assert(f.Has(key));
    }
  }

  {
    Filter f(name, 1, buf, 0, test_keys(128));
    assert(std::filesystem::file_size(filter_file(name, 1)) % kPageSize == 0);

    for (auto &key : test_keys(128)) {
      std::cout << "testing " << key << std::endl;
      assert(f.Has(key));
    }
  }
}
