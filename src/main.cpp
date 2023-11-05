#include <cassert>
#include <exception>
#include <filesystem>
#include <iostream>

#include "dbg.hpp"
#include "filter.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  BufPool buf(
      BufPoolTuning{
          .initial_elements = 4,
          .max_elements = 4,
      },
      std::make_unique<ClockEvictor>(), &Hash);

  PageId id = {.filename = std::string("unique_file"), .page = 2};
  std::cout << Hash(id) << "\n";

  bool exists = std::filesystem::exists("/tmp/MAIN_UNIQUE");
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/MAIN_UNIQUE");
    assert(created);
  }

  auto naming = DbNaming{.dirpath = "/tmp/MAIN_UNIQUE", .name = "MAIN_UNIQUE"};
  std::filesystem::remove(filter_file(naming, 1));

  Filter f(naming, 1, 128, buf, 0);
  f.Put(0);
  assert(f.Has(0) == true);
}
