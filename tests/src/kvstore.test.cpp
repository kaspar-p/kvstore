#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "kvstore.hpp"

#include <gtest/gtest.h>

#include "memtable.hpp"

TEST(KvStore, ScanIncludesEnds)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(1, 3);

  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 3);
  ASSERT_EQ(v[2].second, 30);
}

TEST(KvStore, ScanStopsBeforeEnd)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(1, 2);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);
  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);
}

TEST(KvStore, ScanStopsBeforeStart)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);

  std::vector<std::pair<K, V>> v = table->Scan(2, 3);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 2);
  ASSERT_EQ(v[0].second, 20);
  ASSERT_EQ(v[1].first, 3);
  ASSERT_EQ(v[1].second, 30);
}

TEST(KvStore, ScanGoesBeyondKeySizes)
{
  auto table = std::make_unique<KvStore>();
  table->Put(10, 10);
  table->Put(20, 20);
  table->Put(30, 30);

  std::vector<std::pair<K, V>> v = table->Scan(0, 100);
  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 10);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 20);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 30);
  ASSERT_EQ(v[2].second, 30);
}

TEST(KvStore, InsertAndDeleteOne)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Delete(1);
  const std::optional<V> v = table->Get(1);

  ASSERT_EQ(v, std::nullopt);
}

TEST(KvStore, InsertAndDeleteAFew)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);
  table->Delete(1);

  const std::optional<V> val = table->Get(2);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 20);

  const std::optional<V> val2 = table->Get(1);
  ASSERT_EQ(val2, std::nullopt);
}

TEST(KvStore, InsertAndGetOne)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  const std::optional<V> val = table->Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 10);
}

TEST(KvStore, InsertOneAndReplaceIt)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 100);
  table->Put(1, 200);
  const std::optional<V> val = table->Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 200);
}

TEST(KvStore, InsertManyAndGetMany)
{
  auto table = std::make_unique<KvStore>();
  table->Put(1, 10);
  table->Put(2, 20);
  table->Put(3, 30);
  table->Put(4, 40);

  const std::optional<V> val1 = table->Get(1);
  ASSERT_NE(val1, std::nullopt);
  ASSERT_EQ(val1.value(), 10);

  const std::optional<V> val2 = table->Get(2);
  ASSERT_NE(val2, std::nullopt);
  ASSERT_EQ(val2.value(), 20);

  const std::optional<V> val3 = table->Get(3);
  ASSERT_NE(val3, std::nullopt);
  ASSERT_EQ(val3.value(), 30);

  const std::optional<V> val4 = table->Get(4);
  ASSERT_NE(val4, std::nullopt);
  ASSERT_EQ(val4.value(), 40);
}
