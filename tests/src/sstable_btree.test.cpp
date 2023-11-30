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
#include "testutil.hpp"

TEST(SstableBTree, AddElems) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.AddElems");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  ASSERT_EQ(1, 1);
}

TEST(SstableBTree, GetSingleElems) {
  auto buf = test_buf();
  MemTable memtable(255);
  for (int i = 0; i < 255; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetSingleElems");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

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
  auto buf = test_buf();
  MemTable memtable(2000);
  for (int i = 0; i < 2000; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetManySingleElems");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  for (int i = 0; i < 2000; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), 2 * i);
  }
}

TEST(SstableBTree, GetSingleMissing) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    if (i == 54) continue;
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetSingleMissing");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::optional<V> val = t.GetFromFile(f, 54);
  ASSERT_EQ(val.has_value(), false);

  // out of range
  val = t.GetFromFile(f, 100);
  ASSERT_EQ(val.has_value(), false);
}

TEST(SstableBTree, GetSingleElems2LeafNodes) {
  auto buf = test_buf();
  MemTable memtable(510);
  for (int i = 0; i < 510; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetSingleElems2LeafNodes");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::optional<V> val = t.GetFromFile(f, 509);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 509);
}

TEST(SstableBTree, GetSingleElems2LayersFull) {
  auto buf = test_buf();
  MemTable memtable(65025);
  for (int i = 0; i < 65025; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetSingleElems2LeafNodes");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  for (int i = 0; i < 65025; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_EQ(val.has_value(), true);
    ASSERT_EQ(val.value(), i);
  }
}

TEST(SstableBTree, GetSingleElems3Layers16MB) {
  auto buf = test_buf();
  MemTable memtable(1000000);
  for (int i = 0; i < 1000000; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{buf};
  std::string f("/tmp/SstableBTree.GetSingleElems3Layers16MB.bin");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), 1000000);

  for (int i = 0; i < 1000000; i++) {
   std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_EQ(val.has_value(), true);
    ASSERT_EQ(val.value(), i);
  }
}

// Further test layers of internal nodes (like 3+ layers)
// TODO: taking max of maxes for internal layers

TEST(SstableBTree, Scan10KEntries) {
  auto buf = test_buf();
  int amt = 10000;
  MemTable memtable(amt);
  for (int i = 0; i < amt; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.Scan10KEntries");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, amt);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i), keys->at(i));
  }
}

TEST(SstableBTree, ScanDenseAround) {
  auto buf = test_buf();
  MemTable memtable(100);
  for (int i = 100; i < 200; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeLeftHanging");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, UINT64_MAX);

  ASSERT_EQ(val.size(), 100);
  for (int i = 100; i < 200; i++) {
    ASSERT_EQ(val.at(i - 100).first, i);
    ASSERT_EQ(val.at(i - 100).second, 2 * i);
  }
}

TEST(SstableBTree, ScanDenseRange1LeafNode) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanDenseRange1LeafNode");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);
}

TEST(SstableBTree, ScanDenseRange2LeafNodes) {
  auto buf = test_buf();
  MemTable memtable(510);
  for (int i = 0; i < 510; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanDenseRange2LeafNodes");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 240, 282);
  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 240);
  ASSERT_EQ(val.front().second, 240);
  ASSERT_EQ(val.back().first, 282);
  ASSERT_EQ(val.back().second, 282);
}

TEST(SstableBTree, ScanSparseRangeIncludes) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeIncludes");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

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
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeHuge");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

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
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeLeftHanging");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 10);

  ASSERT_EQ(val.size(), 2);
  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);
}

TEST(SstableBTree, ScanSparseRangeRightHanging) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeRightHanging");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 50, 1000);

  ASSERT_EQ(val.size(), 1);
  ASSERT_EQ(val.at(0).first, 99);
  ASSERT_EQ(val.at(0).second, 109);
}

TEST(SstableBTree, ScanSparseRangeOutOfBounds) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.ScanSparseRangeOutOfBounds");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  // Above range
  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 100, 1000);
  ASSERT_EQ(val.size(), 0);

  // Below range
  val = t.ScanInFile(f, 0, 5);
  ASSERT_EQ(val.size(), 0);
}

TEST(SstableBTree, DrainEmpty) {
  auto buf = test_buf();
  MemTable memtable(64);

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.DrainEmpty");
  std::filesystem::remove(f);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), 0);
}

TEST(SstableBTree, Drain1LeafNodeEntries) {
  auto buf = test_buf();
  int amt = 255;
  MemTable memtable(255);
  for (int i = 0; i < 255; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.Drain10KEntries");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i).first, i);
    ASSERT_EQ(val.at(i).second, 2 * i);
  }
}

TEST(SstableBTree, Drain10KEntries) {
  auto buf = test_buf();
  int amt = 10000;
  MemTable memtable(amt);
  for (int i = 0; i < amt; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.Drain10KEntries");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i).first, i);
    ASSERT_EQ(val.at(i).second, 2 * i);
  }
}

TEST(SstableBTree, GetMinimum) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetMinimum");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  uint64_t minKey = t.GetMinimum(f);
  ASSERT_EQ(minKey, 0);
}

TEST(SstableBTree, GetMaximum) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableBTree t(buf);
  std::string f("/tmp/SstableBTree.GetMaximum");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  uint64_t maxKey = t.GetMaximum(f);
  ASSERT_EQ(maxKey, 63);
}