#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <vector>

#include "minheap.hpp"

TEST(MinHeap, InitWithKeysAndExtract) {
  std::vector<K> initial_keys;
  for (int i = 100; i >= 0 ; --i) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 100 ; i++) {
    std::optional<std::pair<K, int>> min = minheap->Extract();
    ASSERT_EQ(min->first, i);
  }
}

TEST(MinHeap, InitWithNoKeys) {
  std::vector<K> initial_keys;
  auto minheap = std::make_unique<MinHeap>(initial_keys);
}

TEST(MinHeap, InsertAndExtract) {
  std::vector<K> initial_keys;
  for (int i = 100; i >= 0 ; i--) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 100 ; i++) {
    std::pair<K, int> new_pair = std::make_pair(static_cast<K>(101), 1);
    std::optional<std::pair<K, int>> min = minheap->InsertAndExtract(new_pair);
    ASSERT_EQ(min->first, static_cast<K>(i));
  }
}

TEST(MinHeap, InsertAndExtractMany) {
  std::vector<K> initial_keys;
  for (int i = 10 * 1000; i >= 0 ; i--) {
    initial_keys.push_back(K(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 10 * 1000 ; i++) {
    std::pair<K, int> new_pair = std::make_pair(static_cast<K>(10 * 1000 + 1), 1);
    std::optional<std::pair<K, int>> min = minheap->InsertAndExtract(new_pair);
    ASSERT_EQ(min->first, static_cast<K>(i));
  }
}

TEST(MinHeap, InsertAndExtractBreaksTiesWithRunIndex) {
  std::vector<K> initial_keys;
  initial_keys.reserve(100);
  for (int i = 0; i < 100; i++) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);

  std::pair<K, int> new_pair = std::make_pair(static_cast<K>(0), -1);
  std::optional<std::pair<K, int>> min = minheap->InsertAndExtract(new_pair);
  ASSERT_EQ(min->first, 0);
  ASSERT_EQ(min->second, 0);
}

TEST(MinHeap, ExtractBreaksTiesWithRunIndex) {
  std::vector<K> initial_keys;
  initial_keys.reserve(100);
  for (int i = 0; i < 100; i++) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);

  std::pair<K, int> older_pair = std::make_pair(static_cast<K>(1), 0);
  std::optional<std::pair<K, int>> min = minheap->InsertAndExtract(older_pair);
  min = minheap->Extract();
  ASSERT_EQ(min->first, 1);
  ASSERT_EQ(min->second, 1);
  min = minheap->Extract();
  ASSERT_EQ(min->first, 1);
  ASSERT_EQ(min->second, 0);

  std::pair<K, int> newer_pair = std::make_pair(static_cast<K>(3), 4);
  min = minheap->InsertAndExtract(newer_pair);
  min = minheap->Extract();
  ASSERT_EQ(min->first, 3);
  ASSERT_EQ(min->second, 4);
  min = minheap->Extract();
  ASSERT_EQ(min->first, 3);
  ASSERT_EQ(min->second, 3);
}