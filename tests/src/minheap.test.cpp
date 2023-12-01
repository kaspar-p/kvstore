#include "minheap.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

TEST(MinHeapMerge, Simple) {
  {
    std::vector<std::vector<std::pair<K, V>>> buffers{};
    buffers.push_back(std::vector<std::pair<K, V>>({{0, 1}, {2, 3}, {4, 5}}));
    buffers.push_back(std::vector<std::pair<K, V>>({{1, 2}, {3, 4}, {5, 6}}));

    auto result = minheap_merge(buffers);
    ASSERT_EQ(result.size(), 6);
    ASSERT_EQ(result.at(0).first, 0);
    ASSERT_EQ(result.at(0).second, 1);

    ASSERT_EQ(result.at(1).first, 1);
    ASSERT_EQ(result.at(1).second, 2);

    ASSERT_EQ(result.at(2).first, 2);
    ASSERT_EQ(result.at(2).second, 3);

    ASSERT_EQ(result.at(3).first, 3);
    ASSERT_EQ(result.at(3).second, 4);

    ASSERT_EQ(result.at(4).first, 4);
    ASSERT_EQ(result.at(4).second, 5);

    ASSERT_EQ(result.at(5).first, 5);
    ASSERT_EQ(result.at(5).second, 6);
  }

  {
    std::vector<std::vector<std::pair<K, V>>> buffers{};
    buffers.push_back(std::vector<std::pair<K, V>>({{0, 1}, {1, 2}, {2, 3}}));
    buffers.push_back(std::vector<std::pair<K, V>>({{3, 4}, {4, 5}, {5, 6}}));

    auto result = minheap_merge(buffers);
    ASSERT_EQ(result.size(), 6);
    ASSERT_EQ(result.at(0).first, 0);
    ASSERT_EQ(result.at(0).second, 1);

    ASSERT_EQ(result.at(1).first, 1);
    ASSERT_EQ(result.at(1).second, 2);

    ASSERT_EQ(result.at(2).first, 2);
    ASSERT_EQ(result.at(2).second, 3);

    ASSERT_EQ(result.at(3).first, 3);
    ASSERT_EQ(result.at(3).second, 4);

    ASSERT_EQ(result.at(4).first, 4);
    ASSERT_EQ(result.at(4).second, 5);

    ASSERT_EQ(result.at(5).first, 5);
    ASSERT_EQ(result.at(5).second, 6);
  }
}

TEST(MinHeapMerge, OverlappingKeys) {
  {
    std::vector<std::vector<std::pair<K, V>>> buffers{};
    buffers.push_back(std::vector<std::pair<K, V>>({{0, 0}, {1, 2}, {2, 4}}));
    buffers.push_back(std::vector<std::pair<K, V>>({{1, 5}, {2, 10}, {3, 6}}));

    auto result = minheap_merge(buffers);
    ASSERT_EQ(result.size(), 4);

    ASSERT_EQ(result.at(0).first, 0);
    ASSERT_EQ(result.at(0).second, 0);

    ASSERT_EQ(result.at(1).first, 1);
    ASSERT_EQ(result.at(1).second, 5);

    ASSERT_EQ(result.at(2).first, 2);
    ASSERT_EQ(result.at(2).second, 10);

    ASSERT_EQ(result.at(3).first, 3);
    ASSERT_EQ(result.at(3).second, 6);
  }

  {
    std::vector<std::vector<std::pair<K, V>>> buffers{};
    buffers.push_back(std::vector<std::pair<K, V>>({{0, 1}, {1, 2}, {3, 4}}));
    buffers.push_back(std::vector<std::pair<K, V>>({{3, 6}, {4, 5}, {5, 6}}));

    auto result = minheap_merge(buffers);
    ASSERT_EQ(result.size(), 5);
    ASSERT_EQ(result.at(0).first, 0);
    ASSERT_EQ(result.at(0).second, 1);

    ASSERT_EQ(result.at(1).first, 1);
    ASSERT_EQ(result.at(1).second, 2);

    ASSERT_EQ(result.at(2).first, 3);
    ASSERT_EQ(result.at(2).second, 6);

    ASSERT_EQ(result.at(3).first, 4);
    ASSERT_EQ(result.at(3).second, 5);

    ASSERT_EQ(result.at(4).first, 5);
    ASSERT_EQ(result.at(4).second, 6);
  }

  {
    std::vector<std::vector<std::pair<K, V>>> buffers{};
    buffers.push_back(std::vector<std::pair<K, V>>({{0, 1}, {2, 4}, {3, 4}}));
    buffers.push_back(std::vector<std::pair<K, V>>({{3, 6}, {4, 5}, {5, 6}}));

    auto result = minheap_merge(buffers);
    ASSERT_EQ(result.size(), 5);
    ASSERT_EQ(result.at(0).first, 0);
    ASSERT_EQ(result.at(0).second, 1);

    ASSERT_EQ(result.at(1).first, 2);
    ASSERT_EQ(result.at(1).second, 4);

    ASSERT_EQ(result.at(2).first, 3);
    ASSERT_EQ(result.at(2).second, 6);

    ASSERT_EQ(result.at(3).first, 4);
    ASSERT_EQ(result.at(3).second, 5);

    ASSERT_EQ(result.at(4).first, 5);
    ASSERT_EQ(result.at(4).second, 6);
  }
}

TEST(MinHeap, InitWithKeysAndExtract) {
  std::vector<K> initial_keys;
  for (int i = 100; i >= 0; --i) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 100; i++) {
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
  for (int i = 100; i >= 0; i--) {
    initial_keys.push_back(static_cast<K>(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 100; i++) {
    std::pair<K, int> new_pair = std::make_pair(static_cast<K>(101), 1);
    std::optional<std::pair<K, int>> min = minheap->InsertAndExtract(new_pair);
    ASSERT_EQ(min->first, static_cast<K>(i));
  }
}

TEST(MinHeap, InsertAndExtractMany) {
  std::vector<K> initial_keys;
  for (int i = 10 * 1000; i >= 0; i--) {
    initial_keys.push_back(K(i));
  }
  auto minheap = std::make_unique<MinHeap>(initial_keys);
  for (int i = 0; i <= 10 * 1000; i++) {
    std::pair<K, int> new_pair =
        std::make_pair(static_cast<K>(10 * 1000 + 1), 1);
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