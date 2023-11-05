#pragma once

#include <cstdlib>
#include <vector>

#include "constants.hpp"

enum FileType { kManifest = 0, kData = 1, kFilter = 2 };

bool has_magic_numbers(char page[kPageSize], FileType type);
void put_magic_numbers(char page[kPageSize], FileType type);