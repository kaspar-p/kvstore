#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "kvstore.hpp"
#include "naming.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  std::filesystem::remove_all("./kvstore.db");

  KvStore table;
  Options opts = Options{
      .dir = "./",
      .memory_buffer_elements = 100,
      .serialization = DataFileFormat::kFlatSorted,
  };
  table.Open("kvstore.db", opts);

  for (int i = 0; i < 201; i++) {
    table.Put(i, 2 * i);
  }

  std::fstream f("./kvstore.db/kvstore.db.DATA.L0.R0.I0",
                 std::fstream::binary | std::fstream::in);
  SstableNaive s;
  auto v = s.GetFromFile(f, 1);
  assert(v.has_value());

  for (int i = 0; i < 201; i++) {
    std::cout << "testing " << i << '\n';
    auto v = table.Get(i);
    assert(v.has_value());
    assert(v.value() == 2 * i);
  }
}
