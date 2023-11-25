#include "naming.hpp"

#include <gtest/gtest.h>

TEST(Naming, DataFileNamedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  ASSERT_EQ(data_file(naming, 1, 2, 3),
            std::string("dir/kvstore.DATA.L1.R2.I3"));
}

TEST(Naming, DataFileParsedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  std::string filename = data_file(naming, 1, 2, 3);
  ASSERT_EQ(filename, std::string("dir/kvstore.DATA.L1.R2.I3"));

  ASSERT_EQ(parse_data_file_level(filename), 1);
  ASSERT_EQ(parse_data_file_run(filename), 2);
  ASSERT_EQ(parse_data_file_intermediate(filename), 3);
}

TEST(Naming, FilterFileNamedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  ASSERT_EQ(filter_file(naming, 1, 2, 3),
            std::string("dir/kvstore.FILTER.L1.R2.I3"));
}

TEST(Naming, FilterFileParsedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  std::string filename = filter_file(naming, 1, 2, 3);
  ASSERT_EQ(filename, std::string("dir/kvstore.FILTER.L1.R2.I3"));

  ASSERT_EQ(parse_filter_file_level(filename), 1);
  ASSERT_EQ(parse_filter_file_run(filename), 2);
  ASSERT_EQ(parse_filter_file_intermediate(filename), 3);
}

TEST(Naming, ManifestFileNamedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  ASSERT_EQ(manifest_file(naming), std::string("dir/kvstore.MANIFEST"));
}

TEST(Naming, LockFileNamedCorrectly) {
  DbNaming naming = DbNaming{.dirpath = "dir", .name = "kvstore"};
  ASSERT_EQ(lock_file(naming), std::string("dir/kvstore.LOCK"));
}
