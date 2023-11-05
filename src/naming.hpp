#pragma once

#include <cstdint>
#include <string>

std::string manifest_file(std::string dbname) { return dbname + ".MANIFEST"; }

std::string data_file(std::string dbname, uint32_t level, uint32_t run) {
  return dbname + ".DATA.L" + std::to_string(level) + ".R" +
         std::to_string(run);
}

std::string filter_file(std::string dbname, uint32_t level) {
  return dbname + ".FILTER.L" + std::to_string(level);
}
