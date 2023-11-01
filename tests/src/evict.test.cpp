#include <cstdint>
#include <optional>

#include "evict.hpp"

#include <gtest/gtest.h>

#include "buf.hpp"

ChainedPage make_test_page(uint32_t d)
{
  return ChainedPage {
      .page =
          BufferedPage {
              .id = PageId {.level = 0, .run = 0, .page = d},
              .type = kBTreeLeaf,
              .contents = Buffer {},
          },
      .prev = nullptr,
      .next = nullptr,
  };
}

TEST(ClockEviction, NonDirtyMembersEvicted)
{
  ClockEvictor evictor;
  evictor.Resize(1);

  ChainedPage page1 = make_test_page(0);
  std::optional<ChainedPage*> evicted1 = evictor.Insert(&page1);
  ASSERT_EQ(evicted1, std::nullopt);

  ChainedPage page2 = make_test_page(1);
  std::optional<ChainedPage*> evicted2 = evictor.Insert(&page2);
  ASSERT_EQ(evicted2, std::make_optional(&page1));
}

TEST(ClockEviction, DirtyMembersAllKept)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  ChainedPage page1 = make_test_page(1);
  ChainedPage page2 = make_test_page(2);
  ChainedPage page3 = make_test_page(3);
  ChainedPage page4 = make_test_page(4);
  ChainedPage page5 = make_test_page(5);
  ChainedPage page6 = make_test_page(6);

  ASSERT_EQ(evictor.Insert(&page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(&page2), std::nullopt);
  ASSERT_EQ(evictor.Insert(&page3), std::nullopt);
  evictor.MarkUsed(&page1);
  evictor.MarkUsed(&page2);
  evictor.MarkUsed(&page3);
  ASSERT_EQ(evictor.Insert(&page4), std::make_optional(&page1));
  ASSERT_EQ(evictor.Insert(&page5), std::make_optional(&page2));
  ASSERT_EQ(evictor.Insert(&page6), std::make_optional(&page3));
}

TEST(ClockEviction, SomeDirtyMembersKept)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  ChainedPage page1 = make_test_page(1);
  ChainedPage page2 = make_test_page(2);
  ChainedPage page3 = make_test_page(3);
  ChainedPage page4 = make_test_page(4);

  ASSERT_EQ(evictor.Insert(&page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(&page2), std::nullopt);
  ASSERT_EQ(evictor.Insert(&page3), std::nullopt);
  evictor.MarkUsed(&page1);

  ASSERT_EQ(evictor.Insert(&page4), std::make_optional(&page2));
}

TEST(ClockEviction, MarkUnknownUsed)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  auto page0 = make_test_page(0);
  evictor.MarkUsed(&page0);
  ASSERT_TRUE(true);
}