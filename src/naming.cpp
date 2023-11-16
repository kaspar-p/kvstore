#include "naming.hpp"

#include <cstdint>
#include <iostream>
#include <string>

std::string manifest_file(const DbNaming& naming) {
  return naming.dirpath / std::string(naming.name + ".MANIFEST");
}

std::string data_file(const DbNaming& naming, uint32_t level, uint32_t run,
                      uint32_t intermediate) {
  return naming.dirpath /
         std::string(naming.name + ".DATA.L" + std::to_string(level) + ".R" +
                     std::to_string(run) + ".I" + std::to_string(intermediate));
}

std::string filter_file(const DbNaming& naming, uint32_t level, uint32_t run,
                        uint32_t intermediate) {
  return naming.dirpath /
         std::string(naming.name + ".FILTER.L" + std::to_string(level) + ".R" +
                     std::to_string(run) + ".I" + std::to_string(intermediate));
}