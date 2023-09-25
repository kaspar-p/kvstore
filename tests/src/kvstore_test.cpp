#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(MemTable, InsertSeven)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 7;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  EXPECT_EQ(table->Print(),
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
}

TEST(MemTable, InsertNine)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 9;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  EXPECT_EQ(table->Print(),
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
}

TEST(MemTable, InsertTen)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 10;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  EXPECT_EQ(table->Print(),
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
}

TEST(MemTable, InsertTwelve)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 12;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  EXPECT_EQ(table->Print(),
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
}

TEST(MemTable, InsertEighteen)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 18;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  EXPECT_EQ(table->Print(),
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
}

TEST(MemTable, InsertTwentyOne)
{
  auto table = new MemTable<int, std::string>(100);

  int nodes_to_insert = 21;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  // Verified using cs.usfca.edu/~galles/visualization/RedBlack.html
  EXPECT_EQ(table->Print(),
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

  EXPECT_EQ(table->Print(), std::string("{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteAFew)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  table->Delete(1);

  EXPECT_EQ(table->Print(),
            std::string("(b)[2] wow2!\n"
                        "===={NULL}\n"
                        "====(r)[3] wow3!\n"
                        "========{NULL}\n"
                        "========{NULL}\n"));

  auto val = table->Get(2);
  EXPECT_EQ(*val, std::string("wow2!"));
}

TEST(MemTable, InsertAndGetOne)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  const std::string* val = table->Get(1);
  EXPECT_EQ(*val, std::string("wow1!"));
}

TEST(MemTable, InsertOneAndReplaceIt)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "value 1");
  table->Put(1, "value 2");
  const std::string* val = table->Get(1);
  EXPECT_EQ(*val, std::string("value 2"));
}

TEST(MemTable, InsertManyAndGetMany)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  table->Put(4, "wow4!");

  const std::string* val1 = table->Get(1);
  EXPECT_EQ(*val1, std::string("wow1!"));

  const std::string* val2 = table->Get(2);
  EXPECT_EQ(*val2, std::string("wow2!"));

  const std::string* val3 = table->Get(3);
  EXPECT_EQ(*val3, std::string("wow3!"));

  const std::string* val4 = table->Get(4);
  EXPECT_EQ(*val4, std::string("wow4!"));
}