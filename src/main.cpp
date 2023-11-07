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
          .initial_elements = 4096,
          .max_elements = 4096,
      },
      std::make_unique<ClockEvictor>(), &Hash);

  DbNaming name = {.dirpath = "/tmp/MAIN_UNIQUE", .name = "MAIN_UNIQUE"};
  bool exists = std::filesystem::exists("/tmp/MAIN_UNIQUE");
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/MAIN_UNIQUE");
    assert(created);
  }

  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  for (int i = 0; i < 4096; i++) {
    f.Put(i);
  }

  std::cout << buf.DebugPrint() << '\n';

  assert(f.Has(12345678) == true);
}
