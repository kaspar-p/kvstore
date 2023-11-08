#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "memtable.hpp"
#include "sstable.hpp"

TEST(SstableBTree, AddElems) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableBTree t{};
  std::fstream f;
  f.open("/tmp/SstableBtree_AddElems.bin",
         std::fstream::binary | std::fstream::in | std::fstream::out |
             std::fstream::trunc);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  ASSERT_EQ(1, 1);
}

TEST(SstableBTree, GetSingleElems) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  uint64_t buf[kPageSize];
  f.seekg(4096);
  f.read(reinterpret_cast<char*>(buf), kPageSize);
  printf("buf[0] = %lu\n", buf[1]);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);
}