#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "naming.hpp"

class Filter {
 private:
  class FilterImpl;
  const std::unique_ptr<FilterImpl> impl_;

 public:
  /**
   * @brief Construct a bloom filter. See /docs/filter.md for more information.
   *
   * @param dbname The name of the database given when the user calls Open()
   * @param level The level that this Bloom filter is a part of. Assumed to be
   * the only filter for that level, and will contest/override files if there
   * are multiple for a single level.
   * @param buf A buffer pool cache for accessing pages in the filesystem.
   * @param seed A starting random seed for the hash functions in the
   * Blocked BloomFilter.
   * @param keys A large list of keys to write into the bloom filter with. There
   * are no other write methods on this class.
   */
  Filter(const DbNaming& dbname, uint32_t level, BufPool& buf, uint64_t seed,
         std::vector<K> keys);
  ~Filter();

  /**
   * @brief Returns `true` if the filter MIGHT have the key, `false` if the
   * filter DEFINITELY does not have the key.
   */
  [[nodiscard]] bool Has(K key) const;
};
