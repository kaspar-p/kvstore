#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "sstable_naive.hpp"

#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(SstableNaive, w)
{
  MemTable memtable(64);
  for (int i = 0; i < 65; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t {};
  t.flush("s", memtable);
}