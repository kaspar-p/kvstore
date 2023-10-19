#include <cstdlib>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "sstable.hpp"

#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(SstableNaive, AddElems)
{
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t {};
  std::fstream f;
  f.open("/tmp/SstableNaive_AddElems.bin",
         std::fstream::binary | std::fstream::in | std::fstream::out
             | std::fstream::trunc);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  ASSERT_EQ(1, 1);
}

TEST(SstableNaive, GetSingleElems)
{
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.GetSingleElems.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 32);

  ASSERT_EQ(val.has_value(), true);
  ASSERT_EQ(val.value(), 32);
}

TEST(SstableNaive, GetSingleMissing)
{
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.GetSingleMissing.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::optional<V> val = t.GetFromFile(f, 100);

  ASSERT_EQ(val.has_value(), false);
}

TEST(SstableNaive, ScanDenseRange)
{
  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanDenseRange.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
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

TEST(SstableNaive, ScanSparseRangeIncludes)
{
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeIncludes.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

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

TEST(SstableNaive, ScanSparseRangeHuge)
{
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeHuge.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

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

TEST(SstableNaive, ScanSparseRangeLeftHanging)
{
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeLeftHanging.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 10);

  ASSERT_EQ(val.size(), 2);
  ASSERT_EQ(val.at(0).first, 7);
  ASSERT_EQ(val.at(0).second, 17);

  ASSERT_EQ(val.at(1).first, 9);
  ASSERT_EQ(val.at(1).second, 19);
}

TEST(SstableNaive, ScanSparseRangeRightHanging)
{
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeRightHanging.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 50, 1000);

  ASSERT_EQ(val.size(), 1);
  ASSERT_EQ(val.at(0).first, 99);
  ASSERT_EQ(val.at(0).second, 109);
}

TEST(SstableNaive, ScanSparseRangeOutOfBounds)
{
  MemTable memtable(64);
  memtable.Put(7, 17);
  memtable.Put(9, 19);
  memtable.Put(12, 22);
  memtable.Put(15, 25);
  memtable.Put(28, 38);
  memtable.Put(44, 54);
  memtable.Put(99, 109);

  SstableNaive t {};
  std::fstream f("/tmp/SstableNaive.ScanSparseRangeOutOfBounds.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out
                     | std::fstream::trunc);
  ASSERT_EQ(f.is_open(), true);
  ASSERT_EQ(f.good(), true);
  t.Flush(f, memtable);

  // Above range
  std::vector<std::pair<K, V>> val = t.ScanInFile(f, 100, 1000);
  ASSERT_EQ(val.size(), 0);

  // Below range
  val = t.ScanInFile(f, 0, 5);
  ASSERT_EQ(val.size(), 0);
}