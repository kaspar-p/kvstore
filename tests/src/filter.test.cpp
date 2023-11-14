#include "filter.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <filesystem>
#include <fstream>

#include "constants.hpp"
#include "naming.hpp"

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16});
}
BufPool buf = test_buffer();

DbNaming create_dir(std::string dir_name) {
  bool exists = std::filesystem::exists("/tmp/" + dir_name);
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/" + dir_name);
    assert(created);
  }

  return DbNaming{.dirpath = "/tmp/" + dir_name, .name = dir_name};
}

FilterId test_setup(DbNaming &naming) {
  std::filesystem::remove(filter_file(naming, 1, 0));
  return FilterId{
      .dbname = naming,
      .level = 1,
      .run = 0,
  };
}

std::vector<K> test_keys(int num) {
  std::vector<K> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(i);
  }
  return keys;
}

TEST(Filter, Initialization) {
  auto naming = create_dir("Filter.Initialization");
  auto id = test_setup(naming);
  Filter f(id, buf, 0, test_keys(0));
  ASSERT_FALSE(f.Has(0));
  ASSERT_EQ(
      std::filesystem::file_size(filter_file(id.dbname, 1, 0)) % kPageSize, 0);
}

TEST(Filter, PointRead) {
  auto naming = create_dir("Filter.PointRead");
  auto id = test_setup(naming);

  std::vector<K> keys;
  keys.push_back(928137);
  keys.push_back(8778);
  keys.push_back(2891);
  keys.push_back(3289);
  keys.push_back(2183);
  keys.push_back(958572);
  keys.push_back(3982738);
  keys.push_back(837267);
  keys.push_back(1283);
  keys.push_back(32919);
  keys.push_back(309201);
  keys.push_back(283);
  keys.push_back(123);
  keys.push_back(39824738);
  keys.push_back(38763);
  keys.push_back(12058);

  Filter f(id, buf, 0, keys);
  ASSERT_TRUE(f.Has(1283));
  ASSERT_TRUE(f.Has(2891));
  ASSERT_TRUE(f.Has(309201));
  ASSERT_TRUE(f.Has(3982738));
  ASSERT_TRUE(f.Has(12058));
}

TEST(Filter, Recovery) {
  auto naming = create_dir("Filter.Recovery");
  auto id = test_setup(naming);
  std::vector<K> keys = test_keys(128);

  {
    Filter f(id, buf, 0, keys);
    ASSERT_FALSE(f.Has(999));
    ASSERT_EQ(
        std::filesystem::file_size(filter_file(id.dbname, 1, 0)) % kPageSize,
        0);

    for (auto &key : keys) {
      ASSERT_TRUE(f.Has(key));
    }
  }

  {
    Filter f(id, buf, 0);
    ASSERT_EQ(
        std::filesystem::file_size(filter_file(id.dbname, 1, 0)) % kPageSize,
        0);

    for (auto &key : keys) {
      ASSERT_TRUE(f.Has(key));
    }
  }
}

TEST(Filter, RecoveryRandom) {
  auto naming = create_dir("Filter.RecoveryRandom");
  auto id = test_setup(naming);
  auto filename = filter_file(id.dbname, id.level, id.run);

  std::vector<K> keys;
  keys.push_back(928137);
  keys.push_back(8778);
  keys.push_back(2891);
  keys.push_back(3289);
  keys.push_back(2183);
  keys.push_back(958572);
  keys.push_back(3982738);
  keys.push_back(837267);
  keys.push_back(1283);
  keys.push_back(32919);
  keys.push_back(309201);
  keys.push_back(283);
  keys.push_back(123);
  keys.push_back(39824738);
  keys.push_back(38763);
  keys.push_back(12058);

  {
    Filter f(id, buf, 0, keys);
    ASSERT_FALSE(f.Has(999));
    ASSERT_EQ(std::filesystem::file_size(filename) % kPageSize, 0);

    for (int i = 0; i < keys.size(); i++) {
      ASSERT_TRUE(f.Has(keys.at(i)));
    }
  }

  {
    Filter f(id, buf, 0, test_keys(4096));
    ASSERT_EQ(std::filesystem::file_size(filename) % kPageSize, 0);

    for (int i = 0; i < keys.size(); i++) {
      ASSERT_TRUE(f.Has(keys.at(i)));
    }
  }
}

TEST(Filter, FilledFilterGetsFalsePositives) {
  auto naming = create_dir("Filter.FilledFilterGetsFalsePositives");
  auto id = test_setup(naming);
  Filter f(id, buf, 0, test_keys(4096));

  bool has_any = false;
  for (uint64_t i = 4097; i < 999999999; i++) {
    has_any = has_any || f.Has(i);
    if (has_any) {
      break;
    }
  }
  ASSERT_TRUE(has_any);
}

TEST(Filter, DoesNotHaveElement) {
  auto naming = create_dir("Filter.DoesNotHaveElement");
  auto id = test_setup(naming);

  Filter f(id, buf, 0, test_keys(0));
  ASSERT_FALSE(f.Has(1));
}
