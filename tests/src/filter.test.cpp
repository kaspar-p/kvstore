#include "filter.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <filesystem>
#include <fstream>

#include "constants.hpp"
#include "naming.hpp"

BufPool test_buffer() {
  return BufPool(BufPoolTuning{.initial_elements = 2, .max_elements = 16},
                 std::make_unique<ClockEvictor>(), &Hash);
}

DbNaming create_dir(std::string name) {
  bool exists = std::filesystem::exists("/tmp/" + name);
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/" + name);
    assert(created);
  }

  return DbNaming{.dirpath = "/tmp/" + name, .name = name};
}

BufPool buf = test_buffer();

TEST(Filter, Initialization) {
  auto name = create_dir("Filter.Initialization");
  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  ASSERT_FALSE(f.Has(0));
  ASSERT_EQ(std::filesystem::file_size(filter_file(name, 1)) % kPageSize, 0);
}

TEST(Filter, Recovery) {
  auto name = create_dir("Filter.Recovery");
  {
    std::filesystem::remove(filter_file(name, 1));

    Filter f(name, 1, 128, buf, 0);
    ASSERT_FALSE(f.Has(0));
    ASSERT_EQ(std::filesystem::file_size(filter_file(name, 1)) % kPageSize, 0);

    for (int i = 0; i < 100; i++) {
      f.Put(i);
    }
    for (int i = 0; i < 100; i++) {
      ASSERT_TRUE(f.Has(i));
    }
  }

  {
    Filter f(name, 1, 128, buf, 0);
    ASSERT_EQ(std::filesystem::file_size(filter_file(name, 1)) % kPageSize, 0);

    for (int i = 0; i < 100; i++) {
      ASSERT_TRUE(f.Has(i));
    }
  }
}

TEST(Filter, ReadAfterWrite) {
  auto name = create_dir("Filter.ReadAfterWrite");
  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  ASSERT_FALSE(f.Has(0));
  ASSERT_EQ(std::filesystem::file_size(filter_file(name, 1)) % kPageSize, 0);
  ASSERT_EQ(std::filesystem::file_size(filter_file(name, 1)) / kPageSize, 2);

  std::fstream file(filter_file(name, 1));

  char buf1[kPageSize];
  file.seekg(kPageSize);
  file.read(buf1, kPageSize);

  for (int i = 0; i < 100; i++) {
    f.Put(i);
  }

  char buf2[kPageSize];
  file.seekg(kPageSize);
  file.read(buf2, kPageSize);

  bool same = true;
  for (int i = 0; i < kPageSize; i++) {
    if (buf1[i] != buf2[i]) {
      same = false;
    }
  }
  ASSERT_FALSE(same);

  for (int i = 0; i < 100; i++) {
    ASSERT_TRUE(f.Has(i));
  }
}

TEST(Filter, HasElement) {
  auto name = create_dir("Filter.HasElement");
  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  f.Put(0);
  ASSERT_TRUE(f.Has(0));
}

TEST(Filter, FilledFilterGetsFalsePositives) {
  auto name = create_dir("Filter.FilledFilterGetsFalsePositives");
  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  for (int i = 0; i < 4096; i++) {
    f.Put(i);
  }
  ASSERT_TRUE(f.Has(12345678));
}

TEST(Filter, DoesNotHaveElement) {
  auto name = create_dir("Filter.DoesNotHaveElement");
  std::filesystem::remove(filter_file(name, 1));

  Filter f(name, 1, 128, buf, 0);
  f.Put(0);
  ASSERT_FALSE(f.Has(1));
}
