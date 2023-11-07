#include "fileutil.hpp"

#include <cstdlib>
#include <vector>

#include "constants.hpp"

bool has_contents(char page[kPageSize], std::size_t start,
                  std::vector<uint8_t> contents) {
  bool val = true;
  for (int i = 0; i < contents.size(); i++) {
    val &= page[i + start] == contents.at(i);
  }
  return val;
}

bool has_magic_numbers(char page[kPageSize], FileType type) {
  bool has_page_magic =
      has_contents(page, 0, {0x00, 0xdb, 0x00, 0xbe, 0xef, 0x00, 0xdb, 0x00});
  bool has_type_magic = has_contents(
      page, 8,
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, static_cast<uint8_t>(type)});
  return has_page_magic && has_type_magic;
}

void put_contents(char page[kPageSize], std::size_t start,
                  std::vector<uint8_t> contents) {
  for (int i = 0; i < contents.size(); i++) {
    page[i + start] = contents.at(i);
  }
}

void put_magic_numbers(char page[kPageSize], FileType type) {
  put_contents(page, 0, {0x00, 0xdb, 0x00, 0xbe, 0xef, 0x00, 0xdb, 0x00});
  put_contents(
      page, 8,
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, static_cast<uint8_t>(type)});
}
