#include <gtest/gtest.h>

#include "memtable.hpp"

// Demonstrate some basic assertions.
TEST(MemTable, InsertThree)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  EXPECT_EQ(table->Print(),
            std::string("((b)[2] wow2!\n"
                        "----(r)[1] wow1!\n"
                        "--------{NULL}\n"
                        "--------{NULL}\n"
                        "----(r)[3] wow3!\n"
                        "--------{NULL}\n"
                        "--------{NULL}\n"));
}

TEST(MemTable, InsertAndDeleteOne)
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Delete(1);

  EXPECT_EQ(table->Print(), std::string("{NULL}\n"));
}
