#include "dbg.hpp"

#include <gtest/gtest.h>

TEST(AddBitToPrefix, Works) {
  ASSERT_EQ(add_bit_to_prefix(0, 1, 1), 0x40000000);
}

TEST(PrefixBit, WorksAlternating) {
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 0), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 1), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 2), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 3), 0);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 4), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 5), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 6), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 7), 1);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 8), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 9), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 10), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 11), 0);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 12), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 13), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 14), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 15), 1);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 16), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 17), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 18), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 19), 0);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 20), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 21), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 22), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 23), 1);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 24), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 25), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 26), 0);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 27), 0);

  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 28), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 29), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 30), 1);
  ASSERT_EQ(prefix_bit(0x0f0f0f0f, 31), 1);
}

TEST(PrefixBit, FromStart) {
  ASSERT_EQ(prefix_bit(0b10101010101010101010101010101010, 0), 1);
  ASSERT_EQ(prefix_bit(0b10101010101010101010101010101010, 1), 0);
  ASSERT_EQ(prefix_bit(0b10101010101010101010101010101010, 2), 1);
  ASSERT_EQ(prefix_bit(0b10101010101010101010101010101010, 3), 0);
}

TEST(PrefixBit, Partial) {
  ASSERT_EQ(prefix_bit(0b11, 0), 0);
  ASSERT_EQ(prefix_bit(0b11, 1), 0);
  ASSERT_EQ(prefix_bit(0b11, 2), 0);
  ASSERT_EQ(prefix_bit(0b11, 3), 0);

  ASSERT_EQ(prefix_bit(0b11, 28), 0);
  ASSERT_EQ(prefix_bit(0b11, 29), 0);
  ASSERT_EQ(prefix_bit(0b11, 30), 1);
  ASSERT_EQ(prefix_bit(0b11, 31), 1);
}

TEST(PrefixBit, WorksRandom) {
  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 0), 1);
  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 1), 0);

  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 23), 0);
  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 24), 1);

  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 30), 1);
  ASSERT_EQ(prefix_bit(0xaaaaaaaa, 31), 0);
}
