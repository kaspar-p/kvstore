#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "memtable.hpp"
#include "sstable.hpp"

TEST(SstableNaive, AddElems) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t{};
  std::fstream f;
  f.open("/tmp/SstableNaive.AddElems", std::fstream::binary | std::fstream::in |
                                           std::fstream::out |
                                           std::fstream::trunc);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  ASSERT_EQ(1, 1);
}

TEST(SstableNaive, GetSingleElems) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.GetSingleElems",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);
}

TEST(SstableNaive, GetManySingleElems) {
  MemTable memtable(2000);
  for (int i = 0; i < 2000; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.GetManySingleElems",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  for (int i = 0; i < 2000; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), 2 * i);
  }
}

TEST(SstableNaive, GetSingleMissing) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    if (i == 54) continue;
    memtable.Put(i, i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.GetSingleMissing",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::optional<V> val = t.GetFromFile(f, 54);
  ASSERT_EQ(val.has_value(), false);

  // out of range
  val = t.GetFromFile(f, 100);
  ASSERT_EQ(val.has_value(), false);
}

TEST(SstableNaive, ScanDenseRange) {
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanDenseRange",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 10, 52);

  ASSERT_EQ(val.size(), 43);
  ASSERT_EQ(val.front().first, 10);
  ASSERT_EQ(val.front().second, 10);
  ASSERT_EQ(val.back().first, 52);
  ASSERT_EQ(val.back().second, 52);
}

TEST(SstableNaive, ScanSparseRangeIncludes) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeIncludes",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

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
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeHuge",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

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

TEST(SstableNaive, Scan10KEntries) {
  int amt = 10000;
  MemTable memtable(amt);
  for (int i = 0; i < amt; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.Scan10KEntries",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, amt);

  ASSERT_EQ(val.size(), amt);
  for (int i = 0; i < amt; i++) {
    ASSERT_EQ(val.at(i), keys->at(i));
  }
}

TEST(SstableNaive, ScanSparseRangeLeftHanging) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeLeftHanging",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 10);

  ASSERT_EQ(val.size(), 2);
  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);
}

TEST(SstableNaive, ScanDenseAround) {
  MemTable memtable(100);
  for (int i = 100; i < 200; i++) {
    memtable.Put(i, 2 * i);
  }

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeLeftHanging",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, UINT64_MAX);

  ASSERT_EQ(val.size(), 100);
  for (int i = 100; i < 200; i++) {
    ASSERT_EQ(val.at(i - 100).first, i);
    ASSERT_EQ(val.at(i - 100).second, 2 * i);
  }
}

TEST(SstableNaive, ScanSparseRangeRightHanging) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeRightHanging",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 50, 1000);

  ASSERT_EQ(val.size(), 1);
  ASSERT_EQ(val.at(0).first, 99);
  ASSERT_EQ(val.at(0).second, 109);
}

TEST(SstableNaive, ScanSparseRangeOutOfBounds) {
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t{};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeOutOfBounds",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  auto keys = memtable.ScanAll();
  t.Flush(f, *keys);
  // Above range
  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 100, 1000);
  ASSERT_EQ(val.size(), 0);

  // Below range
  val = t.ScanInFile(f, 0, 5);
  ASSERT_EQ(val.size(), 0);
}