#include "naming.hpp"

#include <cstdint>
#include <string>

std::string manifest_file(const DbNaming& naming) {
  return naming.dirpath + "/" + naming.name + ".MANIFEST";
}

std::string data_file(const DbNaming& naming, uint32_t level, uint32_t run) {
  return naming.dirpath + "/" + naming.name + ".DATA.L" +
         std::to_string(level) + ".R" + std::to_string(run);
}

std::string filter_file(const DbNaming& naming, uint32_t level) {
  return naming.dirpath + "/" + naming.name + ".FILTER.L" +
         std::to_string(level);
}