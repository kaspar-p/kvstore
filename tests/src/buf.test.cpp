#include <cstdint>
#include <optional>
#include <string>

#include "buf.hpp"

#include <gtest/gtest.h>

uint32_t test_hash(const PageId& elem)
{
  return elem.page;
}

PageId make_test_id(uint32_t prefix, uint32_t prefix_length)
{
  return PageId {
      .level = 0,
      .run = 0,
      .page = prefix << (32 - prefix_length),
  };
}

PageId make_test_id(uint32_t num)
{
  return PageId {
      .level = 0,
      .run = 0,
      .page = num,
  };
}

TEST(BufPool, GetNonExistantPage)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 1,
      },
      std::make_unique<ClockEvictor>(),
      &test_hash);
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
      std::make_unique<ClockEvictor>(),
      &test_hash);

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

TEST(BufPool, Evolution)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 16,
      },
      std::make_unique<ClockEvictor>(),
      &test_hash);
  PageId id;
  std::string exp;

  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 16\n"
                        "elements: 0\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[]: ()\n"));

  id = make_test_id(0b0111, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)0}});
  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 16\n"
                        "elements: 1\n"
                        "bits: 2\n"
                        "capacity: 4\n"
                        "[]:\n"
                        "..[0]: (0,0,0111) -> ()\n"
                        "..[1]: ()\n"));

  id = make_test_id(0b1010, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)1}});
  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 16\n"
                        "elements: 2\n"
                        "bits: 2\n"
                        "capacity: 4\n"
                        "[]:\n"
                        "..[0]: (0,0,0111) -> ()\n"
                        "..[1]: (0,0,1010) -> ()\n"));

  id = make_test_id(0b1101, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)2}});
  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 16\n"
                        "elements: 3\n"
                        "bits: 3\n"
                        "capacity: 8\n"
                        "[]:\n"
                        "..[0]:\n"
                        "....[00]: ()\n"
                        "....[01]: (0,0,0111) -> ()\n"
                        "..[1]:\n"
                        "....[10]: (0,0,1010) -> ()\n"
                        "....[11]: (0,0,1101) -> ()\n"));

  id = make_test_id(0b1001, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)3}});
  exp = std::string(
      "max: 16\n"
      "elements: 4\n"
      "bits: 3\n"
      "capacity: 8\n"
      "[]:\n"
      "..[0]:\n"
      "....[00]: ()\n"
      "....[01]: (0,0,0111) -> ()\n"
      "..[1]:\n"
      "....[10]: (0,0,1001) -> (0,0,1010) -> ()\n"
      "....[11]: (0,0,1101) -> ()\n");
  ASSERT_EQ(buf.DebugPrint(4), exp);

  id = make_test_id(0b0010, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)4}});
  exp = std::string(
      "max: 16\n"
      "elements: 5\n"
      "bits: 3\n"
      "capacity: 8\n"
      "[]:\n"
      "..[0]:\n"
      "....[00]: (0,0,0010) -> ()\n"
      "....[01]: (0,0,0111) -> ()\n"
      "..[1]:\n"
      "....[10]: (0,0,1001) -> (0,0,1010) -> ()\n"
      "....[11]: (0,0,1101) -> ()\n");
  ASSERT_EQ(buf.DebugPrint(4), exp);

  id = make_test_id(0b0110, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)5}});
  exp = std::string(
      "max: 16\n"
      "elements: 6\n"
      "bits: 4\n"
      "capacity: 16\n"
      "[]:\n"
      "..[0]:\n"
      "....[00]: (0,0,0010) -> ()\n"
      "....[01]: (0,0,0110) -> (0,0,0111) -> ()\n"
      "..[1]:\n"
      "....[10]: (0,0,1001) -> (0,0,1010) -> ()\n"
      "....[11]: (0,0,1101) -> ()\n");
  ASSERT_EQ(buf.DebugPrint(4), exp);

  id = make_test_id(0b1011, 4);
  buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)6}});
  exp = std::string(
      "max: 16\n"
      "elements: 7\n"
      "bits: 4\n"
      "capacity: 16\n"
      "[]:\n"
      "..[0]:\n"
      "....[00]: (0,0,0010) -> ()\n"
      "....[01]: (0,0,0110) -> (0,0,0111) -> ()\n"
      "..[1]:\n"
      "....[10]: (0,0,1011) -> (0,0,1001) -> (0,0,1010) -> ()\n"
      "....[11]: (0,0,1101) -> ()\n");
  ASSERT_EQ(buf.DebugPrint(4), exp);

  for (int i = 0; i < 9; i++) {
    id = make_test_id(i + 7, 4);
    buf.PutPage(id, kBTreeInternal, {std::byte {(uint8_t)(i + 7)}});
  }

  exp = std::string(
      "max: 16\n"
      "elements: 16\n"
      "bits: 4\n"
      "capacity: 16\n"
      "[]:\n"
      "..[0]:\n"
      "....[00]: (0,0,0010) -> ()\n"
      "....[01]: (0,0,0111) -> (0,0,0110) -> (0,0,0111) -> ()\n"
      "..[1]:\n"
      "....[10]: (0,0,1011) -> (0,0,1010) -> (0,0,1001) -> (0,0,1000) -> "
      "(0,0,1011) -> (0,0,1001) -> (0,0,1010) -> ()\n"
      "....[11]: (0,0,1111) -> (0,0,1110) -> (0,0,1101) -> (0,0,1100) -> "
      "(0,0,1101) -> ()\n");
  ASSERT_EQ(buf.DebugPrint(4), exp);
}

TEST(BufPool, InitialSizes)
{
  {
    BufPool buf(
        BufPoolTuning {
            .initial_elements = 4,
            .max_elements = 16,
        },
        std::make_unique<ClockEvictor>(),
        &test_hash);

    ASSERT_EQ(buf.DebugPrint(),
              std::string("max: 16\n"
                          "elements: 0\n"
                          "bits: 2\n"
                          "capacity: 4\n"
                          "[]: ()\n"));
  }

  {
    BufPool buf(
        BufPoolTuning {
            .initial_elements = 7,
            .max_elements = 1000,
        },
        std::make_unique<ClockEvictor>(),
        &test_hash);

    ASSERT_EQ(buf.DebugPrint(),
              std::string("max: 1000\n"
                          "elements: 0\n"
                          "bits: 3\n"
                          "capacity: 8\n"
                          "[]: ()\n"));
  }
}

TEST(BufPool, PagesAreEvictedOnFullRotation)
{
  BufPool buf(
      BufPoolTuning {
          .initial_elements = 1,
          .max_elements = 2,
      },
      std::make_unique<ClockEvictor>(),
      &test_hash);

  ASSERT_EQ(buf.DebugPrint(),
            std::string("max: 2\n"
                        "elements: 0\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[]: ()\n"));

  // Fill the buffer
  PageId id0 = make_test_id(0b001, 3);
  buf.PutPage(id0, kBTreeInternal, {std::byte {0x00}});
  PageId id1 = make_test_id(0b101, 3);
  buf.PutPage(id1, kBTreeInternal, {std::byte {0x01}});

  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 2\n"
                        "elements: 2\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[]: (0,0,1010) -> (0,0,0010) -> ()\n"));

  ASSERT_EQ(buf.GetPage(id0).has_value(), true);
  ASSERT_EQ(buf.GetPage(id1).has_value(), true);

  // Evict the pages by filling the buffer again
  PageId id2 = make_test_id(0b010, 3);
  buf.PutPage(id2, kBTreeInternal, {std::byte {0x02}});

  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 2\n"
                        "elements: 2\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[]: (0,0,0100) -> (0,0,1010) -> ()\n"));

  PageId id3 = make_test_id(0b110, 3);
  buf.PutPage(id3, kBTreeInternal, {std::byte {0x03}});

  ASSERT_EQ(buf.DebugPrint(4),
            std::string("max: 2\n"
                        "elements: 2\n"
                        "bits: 1\n"
                        "capacity: 2\n"
                        "[]: (0,0,1100) -> (0,0,0100) -> ()\n"));

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
