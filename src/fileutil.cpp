#include "fileutil.hpp"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <vector>

#include "constants.hpp"

uint64_t file_magic() { return 0x00db00beef00db00ULL; }

uint64_t type_magic(FileType type) {
  return (0x00000000000000ULL << 8) | static_cast<uint8_t>(type);
}

bool has_contents(uint64_t page[kPageSize / sizeof(uint64_t)],
                  std::size_t start, std::vector<uint64_t> contents) {
  bool val = true;
  for (int i = 0; i < contents.size(); i++) {
    val &= page[i + start] == contents.at(i);
  }
  return val;
}

bool has_magic_numbers(uint64_t page[kPageSize / sizeof(uint64_t)],
                       FileType type) {
  bool has_page_magic = has_contents(page, 0, {file_magic()});
  bool has_type_magic = has_contents(page, 1, {type_magic(type)});
  return has_page_magic && has_type_magic;
}

void put_contents(uint64_t page[kPageSize / sizeof(uint64_t)],
                  std::size_t start, std::vector<uint64_t> contents) {
  for (int i = 0; i < contents.size(); i++) {
    page[i + start] = contents.at(i);
  }
}

void put_magic_numbers(uint64_t page[kPageSize / sizeof(uint64_t)],
                       FileType type) {
  put_contents(page, 0, {file_magic()});
  put_contents(page, 1, {type_magic(type)});
}

bool is_file_type(const std::filesystem::path& file, FileType type) {
  if (!std::filesystem::exists(file)) {
    return false;
  }

  std::fstream f(file, std::fstream::binary | std::fstream::in);
  uint64_t page[kPageSize / sizeof(uint64_t)];
  assert(f.good());
  f.seekg(0);
  f.read(reinterpret_cast<char*>(page), kPageSize);
  assert(f.good());

  return has_magic_numbers(page, type);
}

