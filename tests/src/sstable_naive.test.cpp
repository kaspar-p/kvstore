#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "memtable.hpp"
#include "sstable.hpp"
#include "testutil.hpp"

TEST(SstableNaive, AddElems) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.AddElems";
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  ASSERT_EQ(1, 1);
}

TEST(SstableNaive, GetSingleElems) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.GetSingleElems";
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);
}

TEST(SstableNaive, GetManySingleElems) {
  auto buf = test_buf();

  MemTable memtable(2000);
  for (int i = 0; i < 2000; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.GetManySingleElems";
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  for (int i = 0; i < 2000; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), 2 * i);
  }
}

TEST(SstableNaive, GetSingleMissing) {
  auto buf = test_buf();

  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    if (i == 54) continue;
    memtable.Put(i, i);
  }

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.GetSingleMissing";
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::optional<V> val = t.GetFromFile(f, 54);
  ASSERT_EQ(val.has_value(), false);

  // out of range
  val = t.GetFromFile(f, 100);
  ASSERT_EQ(val.has_value(), false);
}

TEST(SstableNaive, Scan10KEntries) {
  auto buf = test_buf();

  int amt = 10000;
  MemTable memtable(amt);
  for (int i = 0; i < amt; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.Scan10KEntries");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, amt);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i), keys->at(i));
  }
}

TEST(SstableNaive, Scan16MB) {
  auto buf = test_buf();
  MemTable memtable(1000000);
  for (int i = 0; i < 1000000; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t{buf};
  std::string f("/tmp/SstableNaive.Scan16MB.bin");
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), 1000000);
}

TEST(SstableNaive, ScanDenseAround) {
  auto buf = test_buf();
  MemTable memtable(100);
  for (int i = 100; i < 200; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanSparseRangeLeftHanging");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, UINT64_MAX);

  ASSERT_EQ(val.size(), 100);
  for (int i = 100; i < 200; i++) {
    ASSERT_EQ(val.at(i - 100).first, i);
    ASSERT_EQ(val.at(i - 100).second, 2 * i);
  }
}

TEST(SstableNaive, ScanDenseRange) {
  BufPool buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanDenseRange");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);
}

TEST(SstableNaive, ScanSparseRangeIncludes) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanSparseRangeIncludes");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

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

TEST(SstableNaive, ScanSparseRangeHuge) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanSparseRangeHuge");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

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

TEST(SstableNaive, ScanSparseRangeLeftHanging) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanSparseRangeLeftHanging");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 10);

  ASSERT_EQ(val.size(), 2);
  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);
}

TEST(SstableNaive, ScanSparseRangeRightHanging) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t(buf);
  std::string f("/tmp/SstableNaive.ScanSparseRangeRightHanging");
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 50, 1000);

  ASSERT_EQ(val.size(), 1);
  ASSERT_EQ(val.at(0).first, 99);
  ASSERT_EQ(val.at(0).second, 109);
}

TEST(SstableNaive, ScanSparseRangeOutOfBounds) {
  auto buf = test_buf();
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.ScanSparseRangeOutOfBounds";
  auto keys = memtable.ScanAll();
  std::filesystem::remove(f);
  t.Flush(f, *keys, true);

  // Above range
  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 100, 1000);
  ASSERT_EQ(val.size(), 0);

  // Below range
  val = t.ScanInFile(f, 0, 5);
  ASSERT_EQ(val.size(), 0);
}

// Segfaults, fix later
// TEST(SstableNaive, DrainEmpty) {
//   auto buf = test_buf();
//   MemTable memtable(64);

//   SstableNaive t(buf);
//   std::string f = "/tmp/SstableNaive.DrainEmpty";
//   std::filesystem::remove(f);
//   auto keys = memtable.ScanAll();
//   t.Flush(f, *keys, true);

//   std::vector<std::pair<K, V>> val = t.Drain(f);

//   ASSERT_EQ(val.size(), 0);
// }

TEST(SstableNaive, Drain255Entries) {
  auto buf = test_buf();
  int amt = 255;
  MemTable memtable(255);
  for (int i = 0; i < 255; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.Drain10KEntries";
  std::filesystem::remove(f);
  auto keys = memtable.ScanAll();
  std::filesystem::remove(f);
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i).first, i);
    ASSERT_EQ(val.at(i).second, 2 * i);
  }
}

TEST(SstableNaive, Drain10KEntries) {
  auto buf = test_buf();
  int amt = 10000;
  MemTable memtable(amt);
  for (int i = 0; i < amt; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive.Drain10KEntries";
  std::filesystem::remove(f);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys, true);

  std::vector<std::pair<K, V>> val = t.Drain(f);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i).first, i);
    ASSERT_EQ(val.at(i).second, 2 * i);
  }
}

TEST(SstableNaive, GetMinimum) {
  auto buf = test_buf();
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t(buf);
  std::string f = ("/tmp/SstableNaive_GetMinimum");
  std::filesystem::remove(f);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  uint64_t minKey = t.GetMinimum(f);
  ASSERT_EQ(minKey, 0);
}

TEST(SstableNaive, GetMaximum) {
  auto buf = test_buf();

  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t(buf);
  std::string f = "/tmp/SstableNaive_GetMaximum";
  std::filesystem::remove(f);
  auto pairs = memtable.ScanAll();
  t.Flush(f, *pairs, true);

  uint64_t maxKey = t.GetMaximum(f);
  ASSERT_EQ(maxKey, 63);
}