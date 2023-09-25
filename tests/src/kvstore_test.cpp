#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(MemTable, InsertFour)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  table->Put(4, "wow4!");
  EXPECT_EQ(table->Print(),
            std::string("(b)[2] wow2!\n"
                        "----(b)[1] wow1!\n"
                        "--------{NULL}\n"
                        "--------{NULL}\n"
                        "----(b)[3] wow3!\n"
                        "--------{NULL}\n"
                        "--------(r)[4] wow4!\n"
                        "------------{NULL}\n"
                        "------------{NULL}\n"));
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
                        "----{NULL}\n"
                        "----(r)[3] wow3!\n"
                        "--------{NULL}\n"
                        "--------{NULL}\n"));

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