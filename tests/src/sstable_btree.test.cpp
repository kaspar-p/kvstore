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

TEST(SstableBTree, GetSingleElems2LeafNodes) {
  MemTable memtable(510);
  for (int i = 0; i < 510; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems2LeafNodes.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 509);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 509);
}

// Further test layers of internal nodes (like 3+ layers)

TEST(SstableBTree, ScanDenseRange1LeafNode) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanDenseRange1LeafNode.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);
}

TEST(SstableBTree, ScanDenseRange2LeafNodes) {
  MemTable memtable(510);
  for (int i = 0; i < 510; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanDenseRange2LeafNodes.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);

  val = t.ScanInFile(f, 10, 509);

  ASSERT_EQ(val.size(), 509);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 509);
  ASSERT_EQ(val.back().second, 509);
}