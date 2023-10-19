#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "memtable.hpp"

#include <gtest/gtest.h>

#include "kvstore.hpp"

TEST(MemTable, ScanIncludesEnds)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(1, 3);

  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 3);
  ASSERT_EQ(v[2].second, 30);
}

TEST(MemTable, ScanStopsBeforeEnd)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(1, 2);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);
  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);
}

TEST(MemTable, ScanStopsBeforeStart)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(2, 3);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 2);
  ASSERT_EQ(v[0].second, 20);
  ASSERT_EQ(v[1].first, 3);
  ASSERT_EQ(v[1].second, 30);
}

TEST(MemTable, ScanGoesBeyondKeySizes)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(10, 10);
  table->Put(20, 20);
  table->Put(30, 30);

  std::vector<std::pair<K, V>> v = table->Scan(0, 100);
  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 10);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 20);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 30);
  ASSERT_EQ(v[2].second, 30);
}

TEST(MemTable, InsertTooManyThrows)
{
  auto table = std::make_unique<MemTable>(1);
  table->Put(1, 10);

  ASSERT_THROW({ table->Put(2, 20); }, MemTableFullException);
}

TEST(MemTable, InsertOneAndClear)
{
  auto table = std::make_unique<MemTable>(1);
  table->Put(1, 10);
  table->Clear();

  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));
}

TEST(MemTable, InsertAfterClear)
{
  auto table = std::make_unique<MemTable>(2);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Clear();

  table->Put(5, 50);
  table->Put(6, 60);

  ASSERT_EQ(table->Print(),
            std::string("(b)[5] 50\n"
                        "===={NULL}\n"
                        "====(r)[6] 60\n"
                        "========{NULL}\n"
                        "========{NULL}\n"));
}

TEST(MemTable, InsertMany)
{
  auto table = std::make_unique<MemTable>(100);

  int nodes_to_insert = 7;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[2] 200\n"
                        "====(b)[1] 100\n"
                        "========{NULL}\n"
                        "========{NULL}\n"
                        "====(r)[4] 400\n"
                        "========(b)[3] 300\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[6] 600\n"
                        "============(r)[5] 500\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"));
  table->Clear();
  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));

  nodes_to_insert = 9;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] 400\n"
                        "====(r)[2] 200\n"
                        "========(b)[1] 100\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] 300\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(r)[6] 600\n"
                        "========(b)[5] 500\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[8] 800\n"
                        "============(r)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[9] 900\n"
                        "================{NULL}\n"
                        "================{NULL}\n"));
  table->Clear();
  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));

  nodes_to_insert = 10;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] 400\n"
                        "====(b)[2] 200\n"
                        "========(b)[1] 100\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] 300\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(b)[6] 600\n"
                        "========(b)[5] 500\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(r)[8] 800\n"
                        "============(b)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[9] 900\n"
                        "================{NULL}\n"
                        "================(r)[10] 1000\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"));
  table->Clear();
  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));

  nodes_to_insert = 12;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] 400\n"
                        "====(b)[2] 200\n"
                        "========(b)[1] 100\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] 300\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(b)[8] 800\n"
                        "========(r)[6] 600\n"
                        "============(b)[5] 500\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(r)[10] 1000\n"
                        "============(b)[9] 900\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] 1100\n"
                        "================{NULL}\n"
                        "================(r)[12] 1200\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"));
  table->Clear();
  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));

  nodes_to_insert = 18;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[8] 800\n"
                        "====(r)[4] 400\n"
                        "========(b)[2] 200\n"
                        "============(b)[1] 100\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[3] 300\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[6] 600\n"
                        "============(b)[5] 500\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "====(r)[12] 1200\n"
                        "========(b)[10] 1000\n"
                        "============(b)[9] 900\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] 1100\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[14] 1400\n"
                        "============(b)[13] 1300\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[16] 1600\n"
                        "================(b)[15] 1500\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[17] 1700\n"
                        "===================={NULL}\n"
                        "====================(r)[18] 1800\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"));
  table->Clear();
  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));

  nodes_to_insert = 21;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, i * 100);
  }

  // Verified using cs.usfca.edu/~galles/visualization/RedBlack.html
  ASSERT_EQ(table->Print(),
            std::string("(b)[8] 800\n"
                        "====(r)[4] 400\n"
                        "========(b)[2] 200\n"
                        "============(b)[1] 100\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[3] 300\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[6] 600\n"
                        "============(b)[5] 500\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] 700\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "====(r)[12] 1200\n"
                        "========(b)[10] 1000\n"
                        "============(b)[9] 900\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] 1100\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[16] 1600\n"
                        "============(r)[14] 1400\n"
                        "================(b)[13] 1300\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[15] 1500\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "============(r)[18] 1800\n"
                        "================(b)[17] 1700\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[20] 2000\n"
                        "====================(r)[19] 1900\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"
                        "====================(r)[21] 2100\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteOne)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Delete(1);

  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteAFew)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);
  table->Delete(1);

  ASSERT_EQ(table->Print(),
            std::string("(b)[2] 20\n"
                        "===={NULL}\n"
                        "====(r)[3] 30\n"
                        "========{NULL}\n"
                        "========{NULL}\n"));

  auto val = table->Get(2);
  ASSERT_EQ(*val, 20);
}

TEST(MemTable, InsertAndGetOne)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);

  const V* val = table->Get(1);
  ASSERT_NE(val, nullptr);
  ASSERT_EQ(*val, 10);
}

TEST(MemTable, InsertOneAndReplaceIt)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(1, 20);

  const V* val = table->Get(1);
  ASSERT_NE(val, nullptr);
  ASSERT_EQ(*val, 20);
}

TEST(MemTable, InsertManyAndGetMany)
{
  auto table = std::make_unique<MemTable>(100);
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);
  table->Put(4, 40);

  const V* val1 = table->Get(1);
  ASSERT_EQ(*val1, 10);

  const V* val2 = table->Get(2);
  ASSERT_EQ(*val2, 20);

  const V* val3 = table->Get(3);
  ASSERT_EQ(*val3, 30);

  const V* val4 = table->Get(4);
  ASSERT_EQ(*val4, 40);
}