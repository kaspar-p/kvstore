#include "testutil.hpp"

#include <cassert>
#include <filesystem>
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
