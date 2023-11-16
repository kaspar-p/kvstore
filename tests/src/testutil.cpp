#include "testutil.hpp"

#include <cassert>
#include <filesystem>
#include <string>

#include "naming.hpp"

DbNaming create_dir(std::string dir_name) {
  bool exists = std::filesystem::exists("/tmp/" + dir_name);
  if (!exists) {
    bool created = std::filesystem::create_directory("/tmp/" + dir_name);
    assert(created);
  }

  return DbNaming{.dirpath = "/tmp/" + dir_name, .name = dir_name};
}
