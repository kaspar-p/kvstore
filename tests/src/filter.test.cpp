#include "filter.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <filesystem>
#include <fstream>
#include <utility>

#include "constants.hpp"
#include "naming.hpp"
#include "testutil.hpp"

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16});
}

std::vector<std::pair<K, V>> test_keys(int num) {
  std::vector<std::pair<K, V>> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(std::make_pair(i, 2 * i));
  }
  return keys;
}

TEST(Filter, Initialization) {
  auto naming = create_dir("Filter.Initialization");
  auto buf = test_buffer();
  Filter f(naming, buf, 0);

  std::string filt_name = filter_file(naming, 0, 0, 0);
  auto keys = test_keys(0);
  f.Create(filt_name, keys);
  ASSERT_FALSE(f.Has(filt_name, 0));
  ASSERT_EQ(std::filesystem::file_size(filt_name) % kPageSize, 0);
}

TEST(Filter, PointRead) {
  auto naming = create_dir("Filter.PointRead");
  auto buf = test_buffer();

  std::vector<std::pair<K, V>> keys;
  keys.push_back(std::make_pair(928137, 928137));
  keys.push_back(std::make_pair(8778, 8778));
  keys.push_back(std::make_pair(2891, 2891));
  keys.push_back(std::make_pair(3289, 3289));
  keys.push_back(std::make_pair(2183, 2183));
  keys.push_back(std::make_pair(958572, 958572));
  keys.push_back(std::make_pair(3982738, 3982738));
  keys.push_back(std::make_pair(837267, 837267));
  keys.push_back(std::make_pair(1283, 1283));
  keys.push_back(std::make_pair(32919, 32919));
  keys.push_back(std::make_pair(309201, 309201));
  keys.push_back(std::make_pair(283, 283));
  keys.push_back(std::make_pair(123, 123));
  keys.push_back(std::make_pair(39824738, 39824738));
  keys.push_back(std::make_pair(38763, 38763));
  keys.push_back(std::make_pair(12058, 12058));

  Filter f(naming, buf, 0);
  std::string filt_name = filter_file(naming, 0, 0, 0);
  f.Create(filt_name, keys);

  ASSERT_TRUE(f.Has(filt_name, 1283));
  ASSERT_TRUE(f.Has(filt_name, 2891));
  ASSERT_TRUE(f.Has(filt_name, 309201));
  ASSERT_TRUE(f.Has(filt_name, 3982738));
  ASSERT_TRUE(f.Has(filt_name, 12058));
}

TEST(Filter, Recovery) {
  auto naming = create_dir("Filter.Recovery");
  auto buf = test_buffer();
  auto keys = test_keys(128);
  Filter f(naming, buf, 0);
  auto filt_name = filter_file(naming, 0, 0, 0);

  {
    f.Create(filt_name, keys);
    ASSERT_FALSE(f.Has(filt_name, 999));
    ASSERT_EQ(std::filesystem::file_size(filt_name) % kPageSize, 0);
    for (auto &key : keys) {
      ASSERT_TRUE(f.Has(filt_name, key.first));
    }
  }

  {
    ASSERT_EQ(std::filesystem::file_size(filt_name) % kPageSize, 0);
    for (auto &key : keys) {
      ASSERT_TRUE(f.Has(filt_name, key.first));
    }
  }
}

TEST(Filter, RecoveryRandom) {
  auto naming = create_dir("Filter.RecoveryRandom");
  auto buf = test_buffer();
  auto filename = filter_file(naming, 0, 0, 0);

  std::vector<std::pair<K, V>> keys;
  keys.push_back(std::make_pair(928137, 928137));
  keys.push_back(std::make_pair(8778, 8778));
  keys.push_back(std::make_pair(2891, 2891));
  keys.push_back(std::make_pair(3289, 3289));
  keys.push_back(std::make_pair(2183, 2183));
  keys.push_back(std::make_pair(958572, 958572));
  keys.push_back(std::make_pair(3982738, 3982738));
  keys.push_back(std::make_pair(837267, 837267));
  keys.push_back(std::make_pair(1283, 1283));
  keys.push_back(std::make_pair(32919, 32919));
  keys.push_back(std::make_pair(309201, 309201));
  keys.push_back(std::make_pair(283, 283));
  keys.push_back(std::make_pair(123, 123));
  keys.push_back(std::make_pair(39824738, 39824738));
  keys.push_back(std::make_pair(38763, 38763));
  keys.push_back(std::make_pair(12058, 12058));
  Filter f(naming, buf, 0);
  f.Create(filename, keys);

  {
    ASSERT_FALSE(f.Has(filename, 999));
    ASSERT_EQ(std::filesystem::file_size(filename) % kPageSize, 0);

    for (int i = 0; i < keys.size(); i++) {
      ASSERT_TRUE(f.Has(filename, keys.at(i).first));
    }
  }

  {
    ASSERT_EQ(std::filesystem::file_size(filename) % kPageSize, 0);
    for (int i = 0; i < keys.size(); i++) {
      ASSERT_TRUE(f.Has(filename, keys.at(i).first));
    }
  }
}

TEST(Filter, HasVeryMany) {
  auto naming = create_dir("Filter.HasVeryMany");
  auto buf = test_buffer();
  auto filename = filter_file(naming, 0, 0, 0);

  std::vector<std::pair<K, V>> keys = test_keys(10 * 1000);
  Filter f(naming, buf, 20);
  f.Create(filename, keys);

  for (auto const &key : keys) {
    ASSERT_TRUE(f.Has(filename, key.first));
  }
}

TEST(Filter, FilledFilterGetsFalsePositives) {
  auto naming = create_dir("Filter.FilledFilterGetsFalsePositives");
  auto buf = test_buffer();
  Filter f(naming, buf, 0);

  auto filt_name = filter_file(naming, 0, 0, 0);
  auto keys = test_keys(4096);
  f.Create(filt_name, keys);

  bool has_any = false;
  for (uint64_t i = 4097; i < 999999999; i++) {
    has_any = has_any || f.Has(filt_name, i);
    if (has_any) {
      break;
    }
  }
  ASSERT_TRUE(has_any);
}

TEST(Filter, DoesNotHaveElement) {
  auto naming = create_dir("Filter.DoesNotHaveElement");
  auto buf = test_buffer();
  auto keys = test_keys(1024);

  auto filt_name = filter_file(naming, 0, 0, 0);
  Filter f(naming, buf, 0);
  f.Create(filt_name, keys);

  ASSERT_FALSE(f.Has(filt_name, 2048));
}
