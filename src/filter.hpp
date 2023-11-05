#pragma once

#include <cstdint>
#include <memory>
#include <string>

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
   * @param max_elems The maximum number of elements that the filter is meant to
   * support.
   * @param buf A buffer pool cache for accessing pages in the filesystem.
   */
  Filter(const DbNaming& dbname, uint32_t level, uint32_t max_elems,
         BufPool& buf, uint64_t seed);
  ~Filter();

  /**
   * @brief Returns `true` if the filter MIGHT have the key, `false` if the
   * filter DEFINITELY does not have the key.
   */
  [[nodiscard]] bool Has(K key) const;

  /**
   * @brief Puts the key into the set.
   */
  void Put(K key);
};
