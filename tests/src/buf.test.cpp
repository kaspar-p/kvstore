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
          .initial_elements = 1,
          .max_elements = 1,
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
          .initial_elements = 1,
          .max_elements = 1,
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

TEST(BufPool, SupportsManyElements)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 16,
      },
      std::make_unique<ClockEvictor>());

  for (int i = 0; i < 17; i++) {
    PageId id = make_test_id(i);
    buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)i}});
  }

  PageId id = make_test_id(15);
  std::optional<BufferedPage> page = buf.GetPage(id);

  ASSERT_TRUE(page.has_value());
  ASSERT_EQ(page.value().id.level, 0);
  ASSERT_EQ(page.value().id.run, 0);
  ASSERT_EQ(page.value().id.page, 15);
  ASSERT_EQ(page.value().type, kBTreeInternal);
  ASSERT_EQ(page.value().contents, Buffer {std::byte {15}});
}

TEST(BufPool, InitialSizes)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 4,
          .max_elements = 16,
      },
      std::make_unique<ClockEvictor>());

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 0\n"
                        "bits: 2\n"
                        "capacity: 4\n"
                        "[00]: ()\n"
                        "[01]: ()\n"
                        "[10]: ()\n"
                        "[11]: ()\n"));
}

TEST(BufPool, InitialSizesTwo)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 7,
          .max_elements = 1000,
      },
      std::make_unique<ClockEvictor>());

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 1000\n"
                        "elements: 0\n"
                        "bits: 3\n"
                        "capacity: 8\n"
                        "[000]: ()\n"
                        "[001]: ()\n"
                        "[010]: ()\n"
                        "[011]: ()\n"
                        "[100]: ()\n"
                        "[101]: ()\n"
                        "[110]: ()\n"
                        "[111]: ()\n"));
}

TEST(BufPool, ResizeDuringFillingONe)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 16,
      },
      std::make_unique<ClockEvictor>());
  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 0\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[0]: ()\n"
                        "[1]: ()\n"));

  PageId id;
  id = make_test_id(0);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)0}});
  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 1\n"
                        "bits: 2\n"
                        "capacity: 4\n"
                        "[00]: (0,0,0) -> ()\n"
                        "[01]: (0,0,0) -> ()\n"
                        "[10]: ()\n"
                        "[11]: ()\n"));

  id = make_test_id(1);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)1}});
  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 2\n"
                        "bits: 2\n"
                        "capacity: 4\n"
                        "[00]: (0,0,0) -> (0,0,1) -> ()\n"
                        "[01]: (0,0,0) -> (0,0,1) -> ()\n"
                        "[10]: ()\n"
                        "[11]: ()\n"));

  id = make_test_id(2);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)2}});
  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 3\n"
                        "bits: 3\n"
                        "capacity: 8\n"
                        "[000]: (0,0,0) -> ()\n"
                        "[001]: (0,0,1) -> ()\n"
                        "[010]: ()\n"
                        "[011]: ()\n"
                        "[100]: (0,0,2) -> ()\n"
                        "[101]: (0,0,2) -> ()\n"
                        "[110]: ()\n"
                        "[111]: ()\n"));

  id = make_test_id(3);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)3}});
  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 3\n"
                        "bits: 3\n"
                        "capacity: 8\n"
                        "[000]: (0,0,0) -> ()\n"
                        "[001]: (0,0,1) -> ()\n"
                        "[010]: ()\n"
                        "[011]: (0,0,3) -> ()\n"
                        "[100]: (0,0,2) -> ()\n"
                        "[101]: (0,0,2) -> ()\n"
                        "[110]: ()\n"
                        "[111]: ()\n"));
}

TEST(BufPool, SplitDirectoriesOnLocalDepth)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 16,
      },
      std::make_unique<ClockEvictor>());

  for (int i = 0; i < 4; i++) {
    PageId id = make_test_id(i);
    buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)i}});
  }

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 16\n"
                        "elements: 4\n"
                        "bits: 3\n"
                        "capacity: 8\n"
                        "[0]: (0,0,0) -> ()\n"
                        "[1]: (0,0,1) -> ()\n"
                        "[2]: (0,0,2) -> ()\n"
                        "[3]: (0,0,3) -> ()\n"
                        "[4]: ()\n"
                        "[5]: ()\n"
                        "[6]: ()\n"
                        "[7]: ()\n"));
}

TEST(BufPool, PagesAreEvictedOnFullRotation)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 2,
          .max_elements = 2,
      },
      std::make_unique<ClockEvictor>());

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 2\n"
                        "elements: 0\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[0]: ()\n"
                        "[1]: ()\n"));

  // Fill the buffer
  PageId id0 = make_test_id(0);
  buf.PutPage(id0, kBTreeInternal, {std::byte {0x00}});
  PageId id1 = make_test_id(1);
  buf.PutPage(id1, kBTreeInternal, {std::byte {0x01}});

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 2\n"
                        "elements: 2\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[0]: (0,0,0) -> ()\n"
                        "[1]: (0,0,1) -> ()\n"));

  ASSERT_EQ(buf.GetPage(id0).has_value(), true);
  ASSERT_EQ(buf.GetPage(id1).has_value(), true);

  // Evict the pages by filling the buffer again
  PageId id2 = make_test_id(2);
  buf.PutPage(id2, kBTreeInternal, {std::byte {0x02}});
  PageId id3 = make_test_id(3);
  buf.PutPage(id3, kBTreeInternal, {std::byte {0x03}});

  ASSERT_EQ(buf.GetPage(id0).has_value(), false);
  ASSERT_EQ(buf.GetPage(id1).has_value(), false);

  std::optional<BufferedPage> page2 = buf.GetPage(id2);
  ASSERT_EQ(page2.has_value(), true);
  ASSERT_EQ(page2.value().id, id2);
  ASSERT_EQ(page2.value().contents, Buffer {std::byte(0x02)});

  std::optional<BufferedPage> page3 = buf.GetPage(id3);
  ASSERT_EQ(page3.has_value(), true);
  ASSERT_EQ(page3.value().id, id3);
  ASSERT_EQ(page3.value().contents, Buffer {std::byte(0x03)});
}
