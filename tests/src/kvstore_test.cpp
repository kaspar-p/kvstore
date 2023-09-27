#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(MemTable, ScanIncludesEnds)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "v1");
  table->Put(2, "v2");
  table->Put(3, "v3");

  std::vector<std::pair<int, std::string>> v = table->Scan(1, 3);

  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, "v1");

  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, "v2");

  ASSERT_EQ(v[2].first, 3);
  ASSERT_EQ(v[2].second, "v3");
}

TEST(MemTable, ScanStopsBeforeEnd)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "v1");
  table->Put(2, "v2");
  table->Put(3, "v3");

  std::vector<std::pair<int, std::string>> v = table->Scan(1, 2);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, "v1");
  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, "v2");
}

TEST(MemTable, ScanStopsBeforeStart)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "v1");
  table->Put(2, "v2");
  table->Put(3, "v3");

  std::vector<std::pair<int, std::string>> v = table->Scan(2, 3);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 2);
  ASSERT_EQ(v[0].second, "v2");
  ASSERT_EQ(v[1].first, 3);
  ASSERT_EQ(v[1].second, "v3");
}

TEST(MemTable, ScanGoesBeyondKeySizes)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "v1");
  table->Put(2, "v2");
  table->Put(3, "v3");

  std::vector<std::pair<int, std::string>> v = table->Scan(-100, 100);
  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, "v1");

  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, "v2");

  ASSERT_EQ(v[2].first, 3);
  ASSERT_EQ(v[2].second, "v3");
}

TEST(MemTable, InsertTooManyThrows)
{
  auto table = new MemTable<int, std::string>(1);
  table->Put(1, "v1");

  ASSERT_THROW({ table->Put(2, "v2"); }, MemTableFullException);
}

TEST(MemTable, InsertMany)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 7;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[2] val2\n"
                        "====(b)[1] val1\n"
                        "========{NULL}\n"
                        "========{NULL}\n"
                        "====(r)[4] val4\n"
                        "========(b)[3] val3\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[6] val6\n"
                        "============(r)[5] val5\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"));
  delete table;

  table = new MemTable<int, std::string>(100);
  nodes_to_insert = 9;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] val4\n"
                        "====(r)[2] val2\n"
                        "========(b)[1] val1\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] val3\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(r)[6] val6\n"
                        "========(b)[5] val5\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[8] val8\n"
                        "============(r)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[9] val9\n"
                        "================{NULL}\n"
                        "================{NULL}\n"));
  delete table;
  table = new MemTable<int, std::string>(100);

  nodes_to_insert = 10;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] val4\n"
                        "====(b)[2] val2\n"
                        "========(b)[1] val1\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] val3\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(b)[6] val6\n"
                        "========(b)[5] val5\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(r)[8] val8\n"
                        "============(b)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[9] val9\n"
                        "================{NULL}\n"
                        "================(r)[10] val10\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"));
  delete table;
  table = new MemTable<int, std::string>(100);

  nodes_to_insert = 12;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[4] val4\n"
                        "====(b)[2] val2\n"
                        "========(b)[1] val1\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "========(b)[3] val3\n"
                        "============{NULL}\n"
                        "============{NULL}\n"
                        "====(b)[8] val8\n"
                        "========(r)[6] val6\n"
                        "============(b)[5] val5\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(r)[10] val10\n"
                        "============(b)[9] val9\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] val11\n"
                        "================{NULL}\n"
                        "================(r)[12] val12\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"));
  delete table;
  table = new MemTable<int, std::string>(100);

  nodes_to_insert = 18;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  ASSERT_EQ(table->Print(),
            std::string("(b)[8] val8\n"
                        "====(r)[4] val4\n"
                        "========(b)[2] val2\n"
                        "============(b)[1] val1\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[3] val3\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[6] val6\n"
                        "============(b)[5] val5\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "====(r)[12] val12\n"
                        "========(b)[10] val10\n"
                        "============(b)[9] val9\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] val11\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[14] val14\n"
                        "============(b)[13] val13\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(r)[16] val16\n"
                        "================(b)[15] val15\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[17] val17\n"
                        "===================={NULL}\n"
                        "====================(r)[18] val18\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"));
  delete table;
  table = new MemTable<int, std::string>(100);

  nodes_to_insert = 21;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  // Verified using cs.usfca.edu/~galles/visualization/RedBlack.html
  ASSERT_EQ(table->Print(),
            std::string("(b)[8] val8\n"
                        "====(r)[4] val4\n"
                        "========(b)[2] val2\n"
                        "============(b)[1] val1\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[3] val3\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[6] val6\n"
                        "============(b)[5] val5\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[7] val7\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "====(r)[12] val12\n"
                        "========(b)[10] val10\n"
                        "============(b)[9] val9\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "============(b)[11] val11\n"
                        "================{NULL}\n"
                        "================{NULL}\n"
                        "========(b)[16] val16\n"
                        "============(r)[14] val14\n"
                        "================(b)[13] val13\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[15] val15\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "============(r)[18] val18\n"
                        "================(b)[17] val17\n"
                        "===================={NULL}\n"
                        "===================={NULL}\n"
                        "================(b)[20] val20\n"
                        "====================(r)[19] val19\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"
                        "====================(r)[21] val21\n"
                        "========================{NULL}\n"
                        "========================{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteOne)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Delete(1);

  ASSERT_EQ(table->Print(), std::string("{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteAFew)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  table->Delete(1);

  ASSERT_EQ(table->Print(),
            std::string("(b)[2] wow2!\n"
                        "===={NULL}\n"
                        "====(r)[3] wow3!\n"
                        "========{NULL}\n"
                        "========{NULL}\n"));

  auto val = table->Get(2);
  ASSERT_EQ(*val, std::string("wow2!"));
}

TEST(MemTable, InsertAndGetOne)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  const std::string* val = table->Get(1);
  ASSERT_EQ(*val, std::string("wow1!"));
}

TEST(MemTable, InsertOneAndReplaceIt)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "value 1");
  table->Put(1, "value 2");
  const std::string* val = table->Get(1);
  ASSERT_EQ(*val, std::string("value 2"));
}

TEST(MemTable, InsertManyAndGetMany)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  table->Put(4, "wow4!");

  const std::string* val1 = table->Get(1);
  ASSERT_EQ(*val1, std::string("wow1!"));

  const std::string* val2 = table->Get(2);
  ASSERT_EQ(*val2, std::string("wow2!"));

  const std::string* val3 = table->Get(3);
  ASSERT_EQ(*val3, std::string("wow3!"));

  const std::string* val4 = table->Get(4);
  ASSERT_EQ(*val4, std::string("wow4!"));
}