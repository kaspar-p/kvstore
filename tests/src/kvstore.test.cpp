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
#include "sstable.hpp"

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

TEST(KvStore, LevelStructure) {
  std::filesystem::remove_all("/tmp/KvStore.LevelStructure");

  int total_count = 0;

  KvStore table;
  table.Open("KvStore.LevelStructure", Options{
                                           .dir = "/tmp",
                                           .memory_buffer_elements = 2,
                                           .tiers = 4,
                                       });

  std::string prefix = "/tmp/KvStore.LevelStructure/KvStore.LevelStructure.";

  ASSERT_TRUE(std::filesystem::exists(prefix + "LOCK"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "MANIFEST"));

  for (int i = 0; i < 3; i++) {
    std::cout << "[TEST] Putting " << total_count << '\n';
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L0.R0.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L0.R0.I0"));

  for (int i = 0; i < 2; i++) {
    std::cout << "[TEST] Putting " << total_count << '\n';
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L0.R1.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L0.R1.I0"));

  // trigger compaction with two more
  for (int i = 0; i < 4; i++) {
    std::cout << "[TEST] Putting " << total_count << '\n';
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Make sure the previous level got deleted
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R3.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R3.I0"));

  // Make sure the new level has all the same data
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R0.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R0.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R0.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R0.I3"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R0.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R0.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R0.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R0.I3"));

  // fill level 0 again to trigger compaction into level 1
  for (int i = 0; i < 8; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Level 0 should still be empty
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R3.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R3.I0"));

  // Level 1 should have the new run, R1
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R1.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R1.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R1.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R1.I3"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R1.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R1.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R1.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R1.I3"));

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
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R3.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R3.I0"));

  // Level 1 should have the new run, R2
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R2.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R2.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R2.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L1.R2.I3"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R2.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R2.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R2.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L1.R2.I3"));

  // Fill level 0 again, this triggers compaction into L2!
  for (int i = 0; i < 8; i++) {
    table.Put(total_count, 2 * total_count);
    total_count++;
  }

  // Level 0 AND 1 should be empty

  // Level 0
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L0.R3.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L0.R3.I0"));

  // Level 1 Run 0
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R0.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R0.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R0.I3"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R0.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R0.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R0.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R0.I3"));

  // Level 1 Run 1
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R1.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R1.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R1.I3"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R1.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R1.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R1.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R1.I3"));

  // Level 1 Run 2
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R2.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R2.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "DATA.L1.R2.I3"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R2.I0"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R2.I1"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R2.I2"));
  ASSERT_FALSE(std::filesystem::exists(prefix + "FILTER.L1.R2.I3"));

  // Level 2 should have all of the data files in a single run
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I3"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I4"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I5"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I6"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I7"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I8"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I9"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I10"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I11"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I12"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I13"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I14"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "DATA.L2.R0.I15"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I0"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I1"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I2"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I3"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I4"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I5"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I6"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I7"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I8"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I9"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I10"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I11"));

  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I12"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I13"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I14"));
  ASSERT_TRUE(std::filesystem::exists(prefix + "FILTER.L2.R0.I15"));

  for (int i = 0; i < total_count; i++) {
    ASSERT_EQ(table.Get(i), std::make_optional(2 * i));
  }
}

TEST(KvStore, InsertManyAndGetMany) {
  std::filesystem::remove_all("/tmp/KvStore.InsertManyAndGetMany");

  KvStore table;
  table.Open("KvStore.InsertManyAndGetMany", Options{.dir = "/tmp"});
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
                 .memory_buffer_elements = 1000,
                 .serialization = DataFileFormat::kFlatSorted,
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
