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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);

  val = t.GetFromFile(f, 254);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 254);

  val = t.GetFromFile(f, 0);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 0);
}

TEST(SstableBTree, GetManySingleElems) {
  MemTable memtable(2000);
  for (int i = 0; i < 2000; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetManySingleElems",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  for (int i = 0; i < 2000; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), 2 * i);
  }
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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

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
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);

  val = t.ScanInFile(f, 10, 509);

  ASSERT_EQ(val.size(), 500);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 509);
  ASSERT_EQ(val.back().second, 509);
}

TEST(SstableBTree, ScanSparseRangeIncludes) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanSparseRangeIncludes.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 50);

  ASSERT_EQ(val.size(), 4);
  ASSERT_EQ(val.at(0).first, 12);
  ASSERT_EQ(val.at(0).second, 22);

  ASSERT_EQ(val.at(1).first, 15);
  ASSERT_EQ(val.at(1).second, 25);

  ASSERT_EQ(val.at(2).first, 28);
  ASSERT_EQ(val.at(2).second, 38);

  ASSERT_EQ(val.at(3).first, 44);
  ASSERT_EQ(val.at(3).second, 54);
}

TEST(SstableBTree, ScanSparseRangeHuge) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanSparseRangeHuge.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 100);

  ASSERT_EQ(val.size(), 7);

  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);

  ASSERT_EQ(val.at(2).first, 12);
  ASSERT_EQ(val.at(2).second, 22);

  ASSERT_EQ(val.at(3).first, 15);
  ASSERT_EQ(val.at(3).second, 25);

  ASSERT_EQ(val.at(4).first, 28);
  ASSERT_EQ(val.at(4).second, 38);

  ASSERT_EQ(val.at(5).first, 44);
  ASSERT_EQ(val.at(5).second, 54);

  ASSERT_EQ(val.at(6).first, 99);
  ASSERT_EQ(val.at(6).second, 109);
}

TEST(SstableBTree, ScanSparseRangeLeftHanging) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanSparseRangeLeftHanging.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 10);

  ASSERT_EQ(val.size(), 2);
  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);
}

TEST(SstableBTree, ScanSparseRangeRightHanging) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanSparseRangeRightHanging.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 50, 1000);

  ASSERT_EQ(val.size(), 1);
  ASSERT_EQ(val.at(0).first, 99);
  ASSERT_EQ(val.at(0).second, 109);
}

TEST(SstableBTree, ScanSparseRangeOutOfBounds) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.ScanSparseRangeOutOfBounds.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  // Above range
  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 100, 1000);
  ASSERT_EQ(val.size(), 0);

  // Below range
  val = t.ScanInFile(f, 0, 5);
  ASSERT_EQ(val.size(), 0);
}