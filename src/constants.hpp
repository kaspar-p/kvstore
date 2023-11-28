#pragma once

#include <cstdint>
#include <cstdlib>

using K = uint64_t;
using V = uint64_t;

constexpr static std::size_t kKeySize = sizeof(K);
constexpr static std::size_t kValSize = sizeof(V);
constexpr static std::size_t kMegabyteSize = 1024 * 1024;

enum {
  kPageSize = 4096ull,
  BLOCK_NULL = -1,  // TODO: need to change?
  kTombstoneValue = 0x00db00dead00db00ull,
};
