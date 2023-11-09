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
  f.open("/tmp/SstableBTree_AddElems.bin",
         std::fstream::binary | std::fstream::in | std::fstream::out |
             std::fstream::trunc);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  ASSERT_EQ(1, 1);
}

TEST(SstableBTree, GetSingleElems) {
  MemTable memtable(255);
  for (int i = 0; i < 255; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);

  val = t.GetFromFile(f,254);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 254);

  val = t.GetFromFile(f,0);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 0);
}

TEST(SstableBTree, GetSingleMissing) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleMissing.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 100);

  ASSERT_EQ(val.has_value(), false);
}

TEST(SstableBTree, GetSingleElemsLarge) {
  MemTable memtable(512);
  for (int i = 0; i < 512; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElemsLarge.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);

  val = t.GetFromFile(f,254);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 254);

  val = t.GetFromFile(f,0);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 0);
}
// Add tests for when there are internal nodes
// Add tests + write code for scan