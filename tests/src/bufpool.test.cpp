#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "buf.hpp"

#include <gtest/gtest.h>

#include "kvstore.hpp"

Buffer empty_buf()
{
  return {};
};

TEST(BufPool, GetNonExistantPage)
{
  BufPool buf(1, 1);
  std::optional<Buffer> page = buf.GetPage("my file", 0);
  ASSERT_EQ(page, std::nullopt);
}

TEST(BufPool, PagesAreEvictedOnFullRotation)
{
  BufPool buf(1, 1);
  buf.PutPage("my file", 0, {std::byte(0x00)});
  buf.PutPage("my file", 1, {std::byte(0x01)});
  std::optional<Buffer> page = buf.GetPage("my file", 0);
  ASSERT_FALSE(page.has_value());

  page = buf.GetPage("my file", 1);
  ASSERT_EQ(page.value(), (Buffer) {std::byte(0x01)});
}
