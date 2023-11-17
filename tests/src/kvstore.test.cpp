#include "kvstore.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "memtable.hpp"

TEST(KvStore, ScanIncludesEnds) {
  KvStore table;
  table.Open("/tmp/KvStore.ScanIncludesEnds", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 10);
  table.Put(2, 20);
  table.Put(3, 30);

  std::vector<std::pair<K, V>> v = table.Scan(1, 3);

  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 3);
  ASSERT_EQ(v[2].second, 30);
}

TEST(KvStore, ScanStopsBeforeEnd) {
  KvStore table;
  table.Open("/tmp/KvStore.ScanStopsBeforeEnd", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 10);
  table.Put(2, 20);
  table.Put(3, 30);

  std::vector<std::pair<K, V>> v = table.Scan(1, 2);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 1);
  ASSERT_EQ(v[0].second, 10);
  ASSERT_EQ(v[1].first, 2);
  ASSERT_EQ(v[1].second, 20);
}

TEST(KvStore, ScanStopsBeforeStart) {
  KvStore table;
  table.Open("/tmp/KvStore.ScanStopsBeforeStart", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 10);
  table.Put(2, 20);
  table.Put(3, 30);

  std::vector<std::pair<K, V>> v = table.Scan(2, 3);
  ASSERT_EQ(v.size(), 2);
  ASSERT_EQ(v[0].first, 2);
  ASSERT_EQ(v[0].second, 20);
  ASSERT_EQ(v[1].first, 3);
  ASSERT_EQ(v[1].second, 30);
}

TEST(KvStore, ScanGoesBeyondKeySizes) {
  KvStore table;
  table.Open("KvStore.ScanGoesBeyondKeySizes", "/tmp/",
             Options{.overwrite = true});
  table.Put(10, 10);
  table.Put(20, 20);
  table.Put(30, 30);

  std::vector<std::pair<K, V>> v = table.Scan(0, 100);
  ASSERT_EQ(v.size(), 3);

  ASSERT_EQ(v[0].first, 10);
  ASSERT_EQ(v[0].second, 10);

  ASSERT_EQ(v[1].first, 20);
  ASSERT_EQ(v[1].second, 20);

  ASSERT_EQ(v[2].first, 30);
  ASSERT_EQ(v[2].second, 30);
}

TEST(KvStore, UnopenedGetThrow) {
  KvStore db;
  ASSERT_THROW(
      {
        try {
          auto v = db.Get(1);
        } catch (const DatabaseClosedException& e) {
          ASSERT_TRUE(std::string(e.what()).find("closed") !=
                      std::string::npos);
          throw e;
        }
      },
      DatabaseClosedException);
}

TEST(KvStore, UnopenedPutThrow) {
  KvStore db;
  ASSERT_THROW(
      {
        try {
          db.Put(1, 2);
        } catch (const DatabaseClosedException& e) {
          ASSERT_TRUE(std::string(e.what()).find("closed") !=
                      std::string::npos);
          throw e;
        }
      },
      DatabaseClosedException);
}

TEST(KvStore, UnopenedScanThrow) {
  KvStore db;
  ASSERT_THROW(
      {
        try {
          auto v = db.Scan(1, 100);
        } catch (const DatabaseClosedException& e) {
          ASSERT_TRUE(std::string(e.what()).find("closed") !=
                      std::string::npos);
          throw e;
        }
      },
      DatabaseClosedException);
}

TEST(KvStore, TombstoneInsertionThrow) {
  KvStore db;
  db.Open("/tmp/KvStore.TombstoneInsertionThrow", "/tmp/",
          Options{.overwrite = true});
  ASSERT_THROW(
      {
        try {
          db.Put(1, kTombstoneValue);
        } catch (const OnlyTheDatabaseCanUseFunnyValues& e) {
          ASSERT_TRUE(std::string(e.what()).find("funny") != std::string::npos);
          throw e;
        }
      },
      OnlyTheDatabaseCanUseFunnyValues);
}

TEST(KvStore, InsertAndDeleteOne) {
  KvStore table;
  table.Open("KvStore.InsertAndDeleteOne", "/tmp/", Options{.overwrite = true});
  table.Put(1, 10);
  table.Delete(1);

  ASSERT_EQ(table.Get(1), std::nullopt);
}

TEST(KvStore, InsertAndDeleteAFew) {
  KvStore table;
  table.Open("KvStore.InsertAndDeleteAFew", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 10);
  table.Put(2, 20);
  table.Put(3, 30);
  table.Delete(1);

  const std::optional<V> val = table.Get(2);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 20);

  const std::optional<V> val2 = table.Get(1);

  ASSERT_EQ(val2, std::nullopt);
}

TEST(KvStore, InsertAndDeleteThousands) {
  KvStore table;
  table.Open("KvStore.InsertAndDeleteThousands", "/tmp/",
             Options{.overwrite = true});
  for (int i = 0; i < 10 * 1000; i++) {
    std::cout << "Putting " << i << '\n';
    table.Put(i, 10 * i);
  }

  for (int i = 0; i < 10 * 1000; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(10 * i));
  }

  for (int i = 0; i < 10 * 1000; i++) {
    table.Delete(i);
  }

  for (int i = 0; i < 10 * 1000; i++) {
    ASSERT_EQ(table.Get(i), std::nullopt);
  }
}

TEST(KvStore, InsertAndGetOne) {
  KvStore table;
  table.Open("KvStore.InsertAndGetOne", "/tmp/", Options{.overwrite = true});
  table.Put(1, 10);
  const std::optional<V> val = table.Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 10);
}

TEST(KvStore, InsertOneAndReplaceIt) {
  KvStore table;
  table.Open("KvStore.InsertOneAndReplaceIt", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 100);
  table.Put(1, 200);
  const std::optional<V> val = table.Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 200);
}

TEST(KvStore, InsertManyAndGetMany) {
  KvStore table;
  table.Open("KvStore.InsertManyAndGetMany", "/tmp/",
             Options{.overwrite = true});
  table.Put(1, 10);
  table.Put(2, 20);
  table.Put(3, 30);
  table.Put(4, 40);

  const std::optional<V> val1 = table.Get(1);
  ASSERT_NE(val1, std::nullopt);
  ASSERT_EQ(val1.value(), 10);

  const std::optional<V> val2 = table.Get(2);
  ASSERT_NE(val2, std::nullopt);
  ASSERT_EQ(val2.value(), 20);

  const std::optional<V> val3 = table.Get(3);
  ASSERT_NE(val3, std::nullopt);
  ASSERT_EQ(val3.value(), 30);

  const std::optional<V> val4 = table.Get(4);
  ASSERT_NE(val4, std::nullopt);
  ASSERT_EQ(val4.value(), 40);
}
