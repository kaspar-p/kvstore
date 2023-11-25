#include "testutil.hpp"

#include <cassert>
#include <filesystem>
#include <string>

#include "naming.hpp"

DbNaming create_dir(std::string dir_name) {
  std::filesystem::remove_all("/tmp/" + dir_name);
  bool created = std::filesystem::create_directory("/tmp/" + dir_name);
  assert(created);

  return DbNaming{.dirpath = "/tmp/" + dir_name, .name = dir_name};
}
