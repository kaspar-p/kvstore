#include "evict.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>

#include "buf.hpp"

PageId make_test_page(uint32_t d) {
  return PageId{.filename = std::string("file"), .page = d};
}

TEST(ClockEviction, NonDirtyMembersEvicted) {
  ClockEvictor evictor;
  evictor.Resize(1);

  const PageId page1 = make_test_page(0);
  ASSERT_EQ(evictor.Insert(page1), std::nullopt);

  const PageId page2 = make_test_page(1);
  ASSERT_EQ(evictor.Insert(page2).value(), page1);
}

TEST(ClockEviction, DirtyMembersAllKept) {
  ClockEvictor evictor;
  evictor.Resize(3);

  PageId page1 = make_test_page(1);
  PageId page2 = make_test_page(2);
  PageId page3 = make_test_page(3);
  PageId page4 = make_test_page(4);
  PageId page5 = make_test_page(5);
  PageId page6 = make_test_page(6);

  ASSERT_EQ(evictor.Insert(page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(page2), std::nullopt);
  ASSERT_EQ(evictor.Insert(page3), std::nullopt);
  evictor.MarkUsed(page1);
  evictor.MarkUsed(page2);
  evictor.MarkUsed(page3);
  ASSERT_EQ(evictor.Insert(page4).value(), page1);
  ASSERT_EQ(evictor.Insert(page5).value(), page2);
  ASSERT_EQ(evictor.Insert(page6).value(), page3);
}

TEST(ClockEviction, SomeDirtyMembersKeptOne) {
  ClockEvictor evictor;
  evictor.Resize(3);

  PageId page0 = make_test_page(0);
  PageId page1 = make_test_page(1);
  PageId page2 = make_test_page(2);
  PageId page3 = make_test_page(3);

  ASSERT_EQ(evictor.Insert(page0), std::nullopt);
  ASSERT_EQ(evictor.Insert(page1), std::nullopt);
  ASSERT_EQ(evictor.Insert(page2), std::nullopt);
  evictor.MarkUsed(page0);

  ASSERT_EQ(evictor.Insert(page3).value(), page1);
}

TEST(ClockEviction, PutGetPattern) {
  ClockEvictor evictor;
  evictor.Resize(2);

  PageId page0 = make_test_page(0);
  PageId page1 = make_test_page(1);
  PageId page2 = make_test_page(2);
  PageId page3 = make_test_page(3);

  ASSERT_EQ(evictor.Insert(page0), std::nullopt);
  ASSERT_EQ(evictor.Insert(page1), std::nullopt);
  evictor.MarkUsed(page0);
  evictor.MarkUsed(page1);

  ASSERT_EQ(evictor.Insert(page2).value(), page0);
  ASSERT_EQ(evictor.Insert(page3).value(), page1);
}

TEST(ClockEviction, SomeDirtyMembersKeptTwo) {
  ClockEvictor evictor;
  evictor.Resize(2);

  PageId page0 = make_test_page(0);
  PageId page1 = make_test_page(1);
  PageId page2 = make_test_page(2);
  PageId page3 = make_test_page(3);

  ASSERT_EQ(evictor.Insert(page0), std::nullopt);
  ASSERT_EQ(evictor.Insert(page1), std::nullopt);

  ASSERT_EQ(evictor.Insert(page2).value(), page0);
  ASSERT_EQ(evictor.Insert(page3).value(), page1);
}

TEST(ClockEviction, MarkUnknownUsed) {
  ClockEvictor evictor;
  evictor.Resize(3);

  auto page0 = make_test_page(0);
  evictor.MarkUsed(page0);
  ASSERT_TRUE(true);
}