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

FilterId test_setup(DbNaming& naming) {
  std::filesystem::remove(filter_file(naming, 0, 0, 0));

  return FilterId{
      .level = 0,
      .run = 0,
      .intermediate = 0,
  };
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
  auto naming = create_dir("Main.Initialization");
  auto id = test_setup(naming);
  std::cout << "BEFORe!" << std::endl;
  Filter f(naming, id, buf, 0, test_keys(10));
  std::cout << "AFTER!" << std::endl;
  assert(f.Has(0) == false);
  assert(std::filesystem::file_size(
             filter_file(naming, id.level, id.run, id.intermediate)) %
             kPageSize ==
         0);
}
