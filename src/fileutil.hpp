#pragma once

#include <cstdlib>
#include <filesystem>
#include <vector>

#include "constants.hpp"

enum FileType {
  kManifest = 0,
  kData = 1,
  kFilter = 2,
};

uint64_t file_magic();

bool has_magic_numbers(std::array<uint64_t, kPageSize / sizeof(uint64_t)>& page,
                       FileType type);
bool has_magic_numbers(std::vector<uint64_t>& page, FileType type);

void put_magic_numbers(std::array<uint64_t, kPageSize / sizeof(uint64_t)>& page,
                       FileType type);
void put_magic_numbers(std::vector<uint64_t>& page, FileType type);

bool is_file_type(const std::filesystem::path& file, FileType type);
