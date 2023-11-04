#include "filter.hpp"

#include <gtest/gtest.h>

#include "constants.hpp"

TEST(Filter, HasElement)
{
  Filter f(128, 0);
  K key = 0;
  f.Put(key);
  ASSERT_TRUE(f.Has(key));
}

TEST(Filter, FilledFilterGetsFalsePositives)
{
  Filter f(128, 0);
  for (int i = 0; i < 1024; i++) {
    f.Put(i);
  }
  ASSERT_TRUE(f.Has(2048));
}

TEST(Filter, DoesNotHaveElement)
{
  Filter f(128, 0);
  f.Put(0);
  ASSERT_FALSE(f.Has(1));
}
