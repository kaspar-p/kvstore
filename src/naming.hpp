#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

struct DbNaming {
  std::filesystem::path dirpath;
  std::string name;
};

std::string manifest_file(const DbNaming& naming);
std::string data_file(const DbNaming& naming, uint32_t level, uint32_t run);
std::string filter_file(const DbNaming& naming, uint32_t level, uint32_t run);
