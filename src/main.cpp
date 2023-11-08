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
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  assert(f.is_open());
  assert(f.good());
  t.Flush(f, memtable);
  
  uint64_t buf[kPageSize];
  f.seekg(0);
  f.read(reinterpret_cast<char*>(buf), kPageSize);


  std::optional<V> val = t.GetFromFile(f, 32);

  assert(val.has_value());
  assert(val.value() == 32);
}
