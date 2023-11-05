#include "filter.hpp"

#include <gtest/gtest.h>

#include "constants.hpp"

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16},
                 std::make_unique<ClockEvictor>(), &Hash);
}

BufPool buf = test_buffer();

TEST(Filter, HasElement) {
  Filter f("db", 1, 128, buf, 0);
  K key = 0;
  f.Put(key);
  ASSERT_TRUE(f.Has(key));
}

TEST(Filter, FilledFilterGetsFalsePositives) {
  Filter f("db", 1, 128, buf, 0);
  for (int i = 0; i < 1024; i++) {
    f.Put(i);
  }
  ASSERT_TRUE(f.Has(2048));
}

TEST(Filter, DoesNotHaveElement) {
  Filter f("db", 1, 128, buf, 0);
  f.Put(0);
  ASSERT_FALSE(f.Has(1));
}
