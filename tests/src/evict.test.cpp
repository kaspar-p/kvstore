#include <memory>
#include <optional>

#include "evict.hpp"

#include <gtest/gtest.h>

#include "buf.hpp"

std::shared_ptr<BufferedPage> make_test_page(uint32_t d)
{
  return std::make_shared<BufferedPage>(BufferedPage {
      .id = PageId {.level = 0, .run = 0, .page = d},
      .type = kBTreeLeaf,
      .contents = Buffer {},
      .next = std::nullopt,
  });
}

TEST(ClockEviction, NonDirtyMembersEvicted)
{
  ClockEvictor evictor;
  evictor.Resize(1);

  std::shared_ptr<BufferedPage> page1 = make_test_page(0);
  std::optional<std::shared_ptr<BufferedPage>> evicted1 = evictor.Insert(page1);
  ASSERT_EQ(evicted1, std::nullopt);

  std::shared_ptr<BufferedPage> page2 = make_test_page(1);
  std::optional<std::shared_ptr<BufferedPage>> evicted2 = evictor.Insert(page2);
  ASSERT_EQ(evicted2, page1);
}

TEST(ClockEviction, DirtyMembersAllKept)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  std::shared_ptr<BufferedPage> page1 = make_test_page(1);
  std::shared_ptr<BufferedPage> page2 = make_test_page(2);
  std::shared_ptr<BufferedPage> page3 = make_test_page(3);
  std::shared_ptr<BufferedPage> page4 = make_test_page(4);
  std::shared_ptr<BufferedPage> page5 = make_test_page(5);
  std::shared_ptr<BufferedPage> page6 = make_test_page(6);

  ASSERT_EQ(evictor.Insert(page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(page2), std::nullopt);
  ASSERT_EQ(evictor.Insert(page3), std::nullopt);
  evictor.MarkUsed(page1);
  evictor.MarkUsed(page2);
  evictor.MarkUsed(page3);
  ASSERT_EQ(evictor.Insert(page4), page1);
  ASSERT_EQ(evictor.Insert(page5), page2);
  ASSERT_EQ(evictor.Insert(page6), page3);
}

TEST(ClockEviction, SomeDirtyMembersKept)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  std::shared_ptr<BufferedPage> page1 = make_test_page(1);
  std::shared_ptr<BufferedPage> page2 = make_test_page(2);
  std::shared_ptr<BufferedPage> page3 = make_test_page(3);
  std::shared_ptr<BufferedPage> page4 = make_test_page(4);

  ASSERT_EQ(evictor.Insert(page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(page2), std::nullopt);
  ASSERT_EQ(evictor.Insert(page3), std::nullopt);
  evictor.MarkUsed(page1);

  ASSERT_EQ(evictor.Insert(page4), page2);
}

TEST(ClockEviction, MarkUnknownUsed)
{
  ClockEvictor evictor;
  evictor.Resize(3);

  evictor.MarkUsed(make_test_page(0));
  ASSERT_TRUE(true);
}