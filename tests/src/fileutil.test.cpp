#include "fileutil.hpp"

#include <gtest/gtest.h>

#include "constants.hpp"

TEST(PutMagicNumbers, Correct) {
  char buf[kPageSize];
  put_magic_numbers(buf, FileType::kFilter);
  ASSERT_EQ((uint8_t)buf[0], 0x00);
  ASSERT_EQ((uint8_t)buf[1], 0xdb);
  ASSERT_EQ((uint8_t)buf[2], 0x00);
  ASSERT_EQ((uint8_t)buf[3], 0xbe);
  ASSERT_EQ((uint8_t)buf[4], 0xef);
  ASSERT_EQ((uint8_t)buf[5], 0x00);
  ASSERT_EQ((uint8_t)buf[6], 0xdb);
  ASSERT_EQ((uint8_t)buf[7], 0x00);

  ASSERT_EQ((uint8_t)buf[8], 0x00);
  ASSERT_EQ((uint8_t)buf[9], 0x00);
  ASSERT_EQ((uint8_t)buf[10], 0x00);
  ASSERT_EQ((uint8_t)buf[11], 0x00);
  ASSERT_EQ((uint8_t)buf[12], 0x00);
  ASSERT_EQ((uint8_t)buf[13], 0x00);
  ASSERT_EQ((uint8_t)buf[14], 0x00);
  ASSERT_EQ((uint8_t)buf[15], FileType::kFilter);
}
