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
  // MemTable memtable(64);
  // for (int i = 0; i < 64; i++) {
  //   memtable.Put(i, i);
  // }

  // SstableBTree t{};
  // std::fstream f("/tmp/SstableBTree.GetSingleElems.bin",
  //                std::fstream::binary | std::fstream::in | std::fstream::out
  //                |
  //                    std::fstream::trunc);
  // assert(f.is_open());
  // assert(f.good());
  // t.Flush(f, memtable);

  // std::optional<V> val = t.GetFromFile(f, 32);

  // assert(val.has_value());
  // assert(val.value() == 32);

  // std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  // assert(val.size() == 43);
  // assert(val.front().first == 10);
  // assert(val.front().second == 10);
  // assert(val.back().first == 52);
  // assert(val.back().second == 52);

  MemTable memtable(510);
  for (int i = 0; i < 510; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  assert(f.is_open());
  assert(f.good());
  t.Flush(f, memtable);

  // std::optional<V> val = t.GetFromFile(f, 509);

  // assert(val.has_value());
  // assert(val.value() == 509);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 509);

  assert(val.size() == 500);
  assert(val.front().first == 10);
  assert(val.front().second == 10);
  assert(val.back().first == 509);
  assert(val.back().second == 509);
}
