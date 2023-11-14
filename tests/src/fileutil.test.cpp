#include "fileutil.hpp"

#include <gtest/gtest.h>

#include "constants.hpp"

TEST(PutMagicNumbers, Correct) {
  uint64_t buf[kPageSize / sizeof(uint64_t)];
  put_magic_numbers(buf, FileType::kFilter);
  ASSERT_EQ(buf[0], 0x00db00beef00db00ULL);
  ASSERT_EQ(buf[1], (0x00000000000000ULL << 8) | FileType::kFilter);
}

TEST(PutMagicNumbers, PutGet) {
  uint64_t buf[kPageSize / sizeof(uint64_t)];
  put_magic_numbers(buf, FileType::kFilter);
  ASSERT_EQ(buf[0], 0x00db00beef00db00ULL);
  ASSERT_EQ(buf[1], (0x00000000000000ULL << 8) | FileType::kFilter);
  ASSERT_TRUE(has_magic_numbers(buf, FileType::kFilter));
}
