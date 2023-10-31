#include <cstddef>
#include <cstdint>
#include <optional>

#include "buf.hpp"

#include <gtest/gtest.h>

#include "kvstore.hpp"

PageId make_test_id(uint32_t discriminator)
{
  return PageId {
      .level = 0,
      .run = 0,
      .page = discriminator,
  };
}

TEST(BufPool, GetNonExistantPage)
{
  BufPool buf(
      BufPoolTuning {
          .bucket_size = 1,
          .min_directory_size = 1,
          .max_directory_size = 1,
      },
      std::make_unique<ClockEvictor>());
  PageId id = {.level = 0, .run = 1, .page = 2};
  std::optional<BufferedPage> page = buf.GetPage(id);
  ASSERT_EQ(page, std::nullopt);
}

TEST(BufPool, GetRealPage)
{
  BufPool buf(
      BufPoolTuning {
          .bucket_size = 1,
          .min_directory_size = 1,
          .max_directory_size = 1,
      },
      std::make_unique<ClockEvictor>());

  PageId id = {.level = 1, .run = 2, .page = 3};
  buf.PutPage(id, kBTreeInternal, Buffer {});
  std::optional<BufferedPage> page = buf.GetPage(id);

  ASSERT_TRUE(page.has_value());
  ASSERT_EQ(page.value().id.level, 1);
  ASSERT_EQ(page.value().id.run, 2);
  ASSERT_EQ(page.value().id.page, 3);
  ASSERT_EQ(page.value().type, kBTreeInternal);
  ASSERT_EQ(page.value().contents, Buffer {});
}

TEST(BufPool, PagesAreEvictedOnFullRotation)
{
  BufPool buf(
      BufPoolTuning {
          .bucket_size = 1,
          .min_directory_size = 1,
          .max_directory_size = 1,
      },
      std::make_unique<ClockEvictor>());

  // Fill the buffer
  PageId id1 = make_test_id(0);
  buf.PutPage(id1, kBTreeInternal, {std::byte {0x01}});
  PageId id2 = make_test_id(1);
  buf.PutPage(id1, kBTreeInternal, {std::byte {0x02}});

  // Evict the pages by filling the buffer again
  PageId id3 = make_test_id(2);
  buf.PutPage(id2, kBTreeInternal, {std::byte {0x03}});
  PageId id4 = make_test_id(3);
  buf.PutPage(id2, kBTreeInternal, {std::byte {0x04}});

  ASSERT_EQ(buf.GetPage(id1).has_value(), false);
  ASSERT_EQ(buf.GetPage(id2).has_value(), false);

  std::optional<BufferedPage> page3 = buf.GetPage(id3);
  ASSERT_EQ(page3.has_value(), true);
  ASSERT_EQ(page3.value().id, id3);
  ASSERT_EQ(page3.value().contents, Buffer {std::byte(0x03)});

  std::optional<BufferedPage> page4 = buf.GetPage(id4);
  ASSERT_EQ(page4.has_value(), true);
  ASSERT_EQ(page4.value().id, id4);
  ASSERT_EQ(page4.value().contents, Buffer {std::byte(0x03)});
}
