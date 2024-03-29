#include "kvstore.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "memtable.hpp"
#include "sstable.hpp"
#include "testutil.hpp"

TEST(KvStore, ScanIncludesEnds) {
  std::filesystem::remove_all("/tmp/KvStore.ScanIncludesEnds");

  KvStore table;
  table.Open("KvStore.ScanIncludesEnds", Options{.dir = "/tmp"});
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
  std::filesystem::remove_all("/tmp/KvStore.ScanStopsBeforeEnd");

  KvStore table;
  table.Open("KvStore.ScanStopsBeforeEnd", Options{.dir = "/tmp"});
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
  std::filesystem::remove_all("/tmp/KvStore.ScanStopsBeforeStart");

  KvStore table;
  table.Open("KvStore.ScanStopsBeforeStart", Options{.dir = "/tmp"});
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
  std::filesystem::remove_all("/tmp/KvStore.ScanGoesBeyondKeySizes");

  KvStore table;
  table.Open("KvStore.ScanGoesBeyondKeySizes", Options{.dir = "/tmp"});
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
          (void)v;
        } catch (const DatabaseClosedException& e) {
          ASSERT_TRUE(std::string(e.what()).find("closed") !=
                      std::string::npos);
          throw e;
        }
      },
      DatabaseClosedException);
}

TEST(KvStore, DataDirectory) {
  std::filesystem::remove_all("/tmp/KvStore.DataDirectory");

  KvStore db;
  ASSERT_THROW(
      {
        try {
          auto v = db.DataDirectory();
        } catch (const DatabaseClosedException& e) {
          ASSERT_TRUE(std::string(e.what()).find("closed") !=
                      std::string::npos);
          throw e;
        }
      },
      DatabaseClosedException);

  db.Open("KvStore.DataDirectory", Options{.dir = "/tmp"});
  ASSERT_EQ(db.DataDirectory(),
            std::filesystem::path("/tmp/KvStore.DataDirectory"));
}

TEST(KvStore, DatabaseLockExists) {
  std::filesystem::remove_all("/tmp/KvStore.DatabaseLockExists");

  KvStore db;
  db.Open("KvStore.DatabaseLockExists", Options{.dir = "/tmp"});

  ASSERT_TRUE(std::filesystem::exists(
      "/tmp/KvStore.DatabaseLockExists/KvStore.DatabaseLockExists.LOCK"));
  db.Close();

  ASSERT_FALSE(std::filesystem::exists(
      "/tmp/KvStore.DatabaseLockExists/KvStore.DatabaseLockExists.LOCK"));

  db.Open("KvStore.DatabaseLockExists", Options{.dir = "/tmp"});
  ASSERT_TRUE(std::filesystem::exists(
      "/tmp/KvStore.DatabaseLockExists/KvStore.DatabaseLockExists.LOCK"));
}

TEST(KvStore, DatabaseLockedThrowsDifferentInstances) {
  {
    std::filesystem::remove_all(
        "/tmp/KvStore.DatabaseLockedThrowsDifferentInstances/");

    KvStore db;
    db.Open("KvStore.DatabaseLockedThrowsDifferentInstances",
            Options{.dir = "/tmp"});

    KvStore db2;
    ASSERT_TRUE(std::filesystem::exists(
        "/tmp/KvStore.DatabaseLockedThrowsDifferentInstances/"
        "KvStore.DatabaseLockedThrowsDifferentInstances.LOCK"));
    ASSERT_THROW(
        {
          try {
            db2.Open("KvStore.DatabaseLockedThrowsDifferentInstances",
                     Options{.dir = "/tmp"});
          } catch (const DatabaseInUseException& e) {
            ASSERT_TRUE(std::string(e.what()).find("in use") !=
                        std::string::npos);
            throw e;
          }
        },
        DatabaseInUseException);
  }
}

TEST(KvStore, DatabaseLockedThrowsDoubleOpen) {
  {
    std::filesystem::remove_all("/tmp/KvStore.DatabaseLockedThrowsDoubleOpen/");

    KvStore db;
    db.Open("KvStore.DatabaseLockedThrowsDoubleOpen", Options{.dir = "/tmp"});

    ASSERT_TRUE(
        std::filesystem::exists("/tmp/KvStore.DatabaseLockedThrowsDoubleOpen/"
                                "KvStore.DatabaseLockedThrowsDoubleOpen.LOCK"));
    ASSERT_THROW(
        {
          try {
            db.Open("KvStore.DatabaseLockedThrowsDoubleOpen",
                    Options{.dir = "/tmp"});
          } catch (const DatabaseInUseException& e) {
            ASSERT_TRUE(std::string(e.what()).find("in use") !=
                        std::string::npos);
            throw e;
          }
        },
        DatabaseInUseException);
  }
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
  std::filesystem::remove_all("/tmp/KvStore.TombstoneInsertionThrow");

  KvStore db;
  db.Open("KvStore.TombstoneInsertionThrow", Options{.dir = "/tmp"});
  ASSERT_THROW(
      {
        try {
          db.Put(1, kTombstoneValue);
        } catch (const OnlyTheDatabaseCanUseFunnyValuesException& e) {
          ASSERT_TRUE(std::string(e.what()).find("funny") != std::string::npos);
          throw e;
        }
      },
      OnlyTheDatabaseCanUseFunnyValuesException);
}

TEST(KvStore, InsertAndDeleteOne) {
  std::filesystem::remove_all("/tmp/KvStore.InsertAndDeleteOne");

  KvStore table;
  table.Open("KvStore.InsertAndDeleteOne", Options{.dir = "/tmp"});
  table.Put(1, 10);
  table.Delete(1);

  ASSERT_EQ(table.Get(1), std::nullopt);
}

TEST(KvStore, InsertAndDeleteAFew) {
  std::filesystem::remove_all("/tmp/KvStore.InsertAndDeleteAFew");

  KvStore table;
  table.Open("KvStore.InsertAndDeleteAFew", Options{.dir = "/tmp"});
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
  std::filesystem::remove_all("/tmp/KvStore.InsertAndDeleteThousands");

  KvStore table;
  table.Open("KvStore.InsertAndDeleteThousands",
             Options{
                 .dir = "/tmp",
                 .memory_buffer_elements = 100,
                 .serialization = DataFileFormat::kFlatSorted,
             });
  for (int i = 0; i < 10 * 1000; i++) {
    table.Put(i, 10 * i);
  }

  for (int i = 0; i < 10 * 1000; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(10 * i));
    table.Delete(i);
  }

  for (int i = 0; i < 10 * 1000; i++) {
    ASSERT_EQ(table.Get(i), std::nullopt);
  }
}

TEST(KvStore, AssertCacheInvalidation) {
  std::filesystem::remove_all("/tmp/KvStore.AssertCacheInvalidation");

  KvStore table;
  table.Open("KvStore.AssertCacheInvalidation",
             Options{
                 .dir = "/tmp",
                 .memory_buffer_elements = 600,
                 .serialization = DataFileFormat::kFlatSorted,
             });
  for (int i = 0; i < 1000; i++) {
    table.Put(i, 10 * i);
  }
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(10 * i));
    table.Delete(i);
  }
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(table.Get(i), std::nullopt);
  }
}

TEST(KvStore, InsertAndGetOne) {
  std::filesystem::remove_all("/tmp/KvStore.InsertAndGetOne");

  KvStore table;
  table.Open("KvStore.InsertAndGetOne", Options{.dir = "/tmp"});
  table.Put(1, 10);
  const std::optional<V> val = table.Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 10);
}

TEST(KvStore, InsertOneAndReplaceIt) {
  std::filesystem::remove_all("/tmp/KvStore.InsertOneAndReplaceIt");

  KvStore table;
  table.Open("KvStore.InsertOneAndReplaceIt", Options{.dir = "/tmp"});
  table.Put(1, 100);
  table.Put(1, 200);
  const std::optional<V> val = table.Get(1);
  ASSERT_NE(val, std::nullopt);
  ASSERT_EQ(val.value(), 200);
}

TEST(KvStore, InsertLevelsAndReplaceSimple) {
  std::filesystem::remove_all("/tmp/KvStore.InsertLevelsAndReplaceSimple");

  KvStore table;
  Options opts = Options{
      .dir = "/tmp",
      .memory_buffer_elements = 2,
  };
  table.Open("KvStore.InsertLevelsAndReplaceSimple", opts);

  table.Put(1, 100);  // flushed in R0
  table.Put(2, 200);  // flushed in R0
  table.Put(3, 300);  // flushed in R1
  table.Put(1, 400);  // flushed in R1
  table.Put(5, 500);  // in memtable

  ASSERT_EQ(table.Get(1), std::make_optional(400));
  ASSERT_EQ(table.Get(2), std::make_optional(200));
  ASSERT_EQ(table.Get(3), std::make_optional(300));
  ASSERT_EQ(table.Get(4), std::nullopt);
  ASSERT_EQ(table.Get(5), std::make_optional(500));
}

TEST(KvStore, ScanAcrossLevelsSimpleInOrder) {
  std::filesystem::remove_all("/tmp/KvStore.ScanAcrossRunsSimple");

  KvStore table;
  Options opts = Options{
      .dir = "/tmp",
      .memory_buffer_elements = 2,
  };
  table.Open("KvStore.ScanAcrossRunsSimple", opts);

  for (std::size_t i = 10; i < 90; i++) {
    table.Put(i, 2 * i);
  }

  // Centered
  auto v = table.Scan(20, 60);
  ASSERT_EQ(v.size(), 41);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 20);
    ASSERT_EQ(v.at(i).second, 2 * (i + 20));
  }

  // Left hanging
  v = table.Scan(0, 20);
  ASSERT_EQ(v.size(), 11);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 10);
    ASSERT_EQ(v.at(i).second, 2 * (i + 10));
  }

  // Right hanging
  v = table.Scan(50, 150);
  ASSERT_EQ(v.size(), 40);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 50);
    ASSERT_EQ(v.at(i).second, 2 * (i + 50));
  }

  // Both hanging
  v = table.Scan(0, 150);
  ASSERT_EQ(v.size(), 80);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 10);
    ASSERT_EQ(v.at(i).second, 2 * (i + 10));
  }
}

TEST(KvStore, ScanAcrossLevelsSimpleOutOfOrder) {
  std::filesystem::remove_all("/tmp/KvStore.ScanAcrossRunsSimple");

  KvStore table;
  Options opts = Options{
      .dir = "/tmp",
      .memory_buffer_elements = 2,
  };
  table.Open("KvStore.ScanAcrossRunsSimple", opts);

  std::vector<uint64_t> vals{};
  for (uint64_t i = 10; i < 90; i++) {
    vals.push_back(i);
  }

  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(vals.begin(), vals.end(), g);

  // Add to the table
  for (const auto& i : vals) {
    table.Put(i, 2 * i);
  }

  // Centered
  auto v = table.Scan(20, 60);
  ASSERT_EQ(v.size(), 41);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 20);
    ASSERT_EQ(v.at(i).second, 2 * (i + 20));
  }

  // Left hanging
  v = table.Scan(0, 20);
  ASSERT_EQ(v.size(), 11);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 10);
    ASSERT_EQ(v.at(i).second, 2 * (i + 10));
  }

  // Right hanging
  v = table.Scan(50, 150);
  ASSERT_EQ(v.size(), 40);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 50);
    ASSERT_EQ(v.at(i).second, 2 * (i + 50));
  }

  // Both hanging
  v = table.Scan(0, 150);
  ASSERT_EQ(v.size(), 80);
  for (std::size_t i = 0; i < v.size(); i++) {
    ASSERT_EQ(v.at(i).first, i + 10);
    ASSERT_EQ(v.at(i).second, 2 * (i + 10));
  }
}

TEST(KvStore, DeleteFromLevelsSimple) {
  std::filesystem::remove_all("/tmp/KvStore.DeleteFromLevelsSimple");

  KvStore table;
  Options opts = Options{
      .dir = "/tmp",
      .memory_buffer_elements = 2,
  };
  table.Open("KvStore.DeleteFromLevelsSimple", opts);

  table.Put(1, 100);  // flushed in R0
  table.Put(2, 200);  // flushed in R0
  table.Put(5, 500);  // flushed in R1
  table.Delete(1);    // flushed in R1
  table.Put(2, 400);  // in memtable

  ASSERT_EQ(table.Get(1), std::nullopt);
  ASSERT_EQ(table.Get(2), std::make_optional(400));
  ASSERT_EQ(table.Get(3), std::nullopt);
  ASSERT_EQ(table.Get(5), std::make_optional(500));
}

int keys_to_add_run_to_level(uint32_t memtable_capacity, uint8_t tiers,
                             int level) {
  return (memtable_capacity * pow(tiers, level));
}

int keys_to_fill_level(uint32_t memtable_capacity, uint8_t tiers, int level) {
  return (memtable_capacity * pow(tiers, level)) + memtable_capacity;
}

TEST(KvStore, LevelStructure) {
  std::filesystem::remove_all("/tmp/KvStore.LevelStructure");

  int total_count = 0;

  KvStore table;
  uint8_t tiers = 4;
  uint32_t memtable_capacity = 2;
  table.Open("KvStore.LevelStructure",
             Options{
                 .dir = "/tmp",
                 .memory_buffer_elements = memtable_capacity,
                 .tiers = tiers,
             });

  std::string prefix = "/tmp/KvStore.LevelStructure/KvStore.LevelStructure.";

  ASSERT_TRUE(std::filesystem::exists(prefix + "LOCK"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "MANIFEST"));

  for (int i = 0; i < 3; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_exists(prefix, tiers, 0, 0);

  for (int i = 0; i < 2; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_exists(prefix, tiers, 0, 1);

  // trigger compaction with two more
  for (int i = 0; i < 4; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Make sure the previous level got deleted
  structure_not_exists(prefix, tiers, 0);

  // Make sure the new level has all the same data
  structure_exists(prefix, tiers, 1, 0);

  // fill level 0 again to trigger compaction into level 1
  for (int i = 0; i < 8; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Level 0 should still be empty
  structure_not_exists(prefix, tiers, 0);

  // Level 1 should have the new run, R1
  structure_exists(prefix, tiers, 1, 1);

  // Make sure all of the data still reachable
  for (int i = 0; i < total_count; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(2 * i));
  }

  // Fill level 0 again
  for (int i = 0; i < 8; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Level 0 should still be empty
  structure_not_exists(prefix, tiers, 0);

  // Level 1 should have the new run, R2
  structure_exists(prefix, tiers, 1, 2);

  // Fill level 0 again, this triggers compaction into L2!
  for (int i = 0; i < 8; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Level 0 AND 1 should be empty
  structure_not_exists(prefix, tiers, 0);
  structure_not_exists(prefix, tiers, 1);

  // Level 2 should have all of the data files in a single run
  structure_exists(prefix, tiers, 2, 0);
  structure_not_exists(prefix, tiers, 2, 1);
  structure_not_exists(prefix, tiers, 2, 2);

  for (int i = 0; i < total_count; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(2 * i));
  }

  // Insert enough to get to an entirely full level 5, level 4, ...

  // Fill Level 2 Run 1
  for (int i = 0; i < keys_to_add_run_to_level(memtable_capacity, tiers, 2);
       i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Fill Level 2 Run 2
  for (int i = 0; i < keys_to_add_run_to_level(memtable_capacity, tiers, 2);
       i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_not_exists(prefix, tiers, 0);
  structure_not_exists(prefix, tiers, 1);

  // Level 2 should have all of the data files in a single run
  structure_exists(prefix, tiers, 2, 0);
  structure_exists(prefix, tiers, 2, 1);
  structure_exists(prefix, tiers, 2, 2);

  for (int i = 0; i < keys_to_add_run_to_level(memtable_capacity, tiers, 2);
       i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_not_exists(prefix, tiers, 0);
  structure_not_exists(prefix, tiers, 1);
  structure_not_exists(prefix, tiers, 2);

  // Level 3 should have all of the data files in a single run
  structure_exists(prefix, tiers, 3, 0);
  structure_not_exists(prefix, tiers, 3, 1);
  structure_not_exists(prefix, tiers, 3, 2);

  // Fill level 3
  for (int i = 0; i < 2 * keys_to_add_run_to_level(memtable_capacity, tiers, 3);
       i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_exists(prefix, tiers, 3, 0);
  structure_exists(prefix, tiers, 3, 1);
  structure_exists(prefix, tiers, 3, 2);

  // Ensure data integrity
  for (int i = 0; i < total_count; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(2 * i));
  }
}

TEST(KvStore, BatchLevelStructure) {
  std::filesystem::remove_all("/tmp/KvStore.BatchLevelStructure");

  KvStore table;
  uint8_t tiers = 2;
  uint32_t memtable_capacity = 10;
  table.Open("KvStore.BatchLevelStructure",
             Options{
                 .dir = "/tmp",
                 .memory_buffer_elements = memtable_capacity,
                 .tiers = tiers,
             });

  std::string prefix =
      "/tmp/KvStore.BatchLevelStructure/KvStore.BatchLevelStructure.";

  ASSERT_TRUE(std::filesystem::exists(prefix + "LOCK"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "MANIFEST"));
  int total_count = 0;

  table.Put(0, 0);
  total_count++;

  for (int i = 0; i < keys_to_add_run_to_level(memtable_capacity, tiers, 7);
       i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  structure_not_exists(prefix, tiers, 0);
  structure_not_exists(prefix, tiers, 1);
  structure_not_exists(prefix, tiers, 2);
  structure_not_exists(prefix, tiers, 3);
  structure_not_exists(prefix, tiers, 4);
  structure_not_exists(prefix, tiers, 5);
  structure_not_exists(prefix, tiers, 6);
  structure_exists(prefix, tiers, 7, 0);

  for (int i = 0; i < total_count; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(2 * i));
  }
}

TEST(KvStore, InsertManyAndGetMany) {
  std::filesystem::remove_all("/tmp/KvStore.InsertManyAndGetMany");

  KvStore table;
  table.Open("KvStore.InsertManyAndGetMany", Options{
                                                 .dir = "/tmp",
                                                 .memory_buffer_elements = 100,
                                             });
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

TEST(KvStore, InsertVeryManyAndGet) {
  std::filesystem::remove_all("/tmp/KvStore.InsertVeryManyAndGet");

  KvStore table;
  table.Open("KvStore.InsertVeryManyAndGet",
             Options{
                 .dir = "/tmp",
                 .memory_buffer_elements = 30,
                 .serialization = DataFileFormat::kBTree,
             });
  for (int i = 0; i < 10 * 1000; i++) {
    table.Put(i, 2 * i);
  }

  for (int i = 0; i < 10 * 1000; i++) {
    const auto val = table.Get(i);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), 2 * i);
  }
}
