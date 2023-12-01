#include "testutil.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <filesystem>
#include <cmath>
#include <string>

#include "buf.hpp"
#include "naming.hpp"

DbNaming create_dir(std::string dir_name) {
  std::filesystem::remove_all("/tmp/" + dir_name);
  bool created = std::filesystem::create_directory("/tmp/" + dir_name);
  assert(created);

  return DbNaming{.dirpath = "/tmp/" + dir_name, .name = dir_name};
}

BufPool test_buf() {
  return {BufPoolTuning{.initial_elements = 2, .max_elements = 16}};
}

void syntactic_exists(std::string file) {
  bool exists = std::filesystem::exists(file);
  if (exists) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_EQ(file, "exists!");
  }
}

void syntactic_not_exists(std::string file) {
  bool exists = std::filesystem::exists(file);
  if (!exists) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_EQ(file, "not exists!");
  }
}

void structure_exists(std::string prefix, uint8_t tiers, int level, int run,
                      int intermediate) {
  (void)tiers;
  syntactic_exists(prefix + "DATA.L" + std::to_string(level) + ".R" +
                   std::to_string(run) + ".I" + std::to_string(intermediate));
  syntactic_exists(prefix + "FILTER.L" + std::to_string(level) + ".R" +
                   std::to_string(run) + ".I" + std::to_string(intermediate));
}

void structure_exists(std::string prefix, uint8_t tiers, int level, int run) {
  for (int i = 0; i < pow(tiers, level); i++) {
    std::string sstable = prefix + "DATA.L" + std::to_string(level) + ".R" +
                          std::to_string(run) + ".I" + std::to_string(i);
    syntactic_exists(sstable);
    std::string filter = prefix + "FILTER.L" + std::to_string(level) + ".R" +
                         std::to_string(run) + ".I" + std::to_string(i);
    syntactic_exists(filter);
  }
}

void structure_exists(std::string prefix, uint8_t tiers, int level) {
  for (int run = 0; run < tiers - 1; run++) {
    for (int i = 0; i < pow(tiers, level); i++) {
      syntactic_exists(prefix + "DATA.L" + std::to_string(level) + ".R" +
                       std::to_string(run) + ".I" + std::to_string(i));
      syntactic_exists(prefix + "FILTER.L" + std::to_string(level) + ".R" +
                       std::to_string(run) + ".I" + std::to_string(i));
    }
  }
}

void structure_not_exists(std::string prefix, uint8_t tiers, int level, int run,
                          int intermediate) {
  (void)tiers;
  syntactic_not_exists(prefix + "DATA.L" + std::to_string(level) + ".R" +
                       std::to_string(run) + ".I" +
                       std::to_string(intermediate));
  syntactic_not_exists(prefix + "FILTER.L" + std::to_string(level) + ".R" +
                       std::to_string(run) + ".I" +
                       std::to_string(intermediate));
}

void structure_not_exists(std::string prefix, uint8_t tiers, int level,
                          int run) {
  for (int i = 0; i < pow(tiers, level); i++) {
    std::string sstable = prefix + "DATA.L" + std::to_string(level) + ".R" +
                          std::to_string(run) + ".I" + std::to_string(i);
    syntactic_not_exists(sstable);
    std::string filter = prefix + "FILTER.L" + std::to_string(level) + ".R" +
                         std::to_string(run) + ".I" + std::to_string(i);
    syntactic_not_exists(filter);
  }
}

void structure_not_exists(std::string prefix, uint8_t tiers, int level) {
  for (int run = 0; run < tiers - 1; run++) {
    for (int i = 0; i < pow(tiers, level); i++) {
      syntactic_not_exists(prefix + "DATA.L" + std::to_string(level) + ".R" +
                           std::to_string(run) + ".I" + std::to_string(i));
      syntactic_not_exists(prefix + "FILTER.L" + std::to_string(level) + ".R" +
                           std::to_string(run) + ".I" + std::to_string(i));
    }
  }
}