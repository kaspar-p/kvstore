#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "naming.hpp"

struct FilterId {
  DbNaming& dbname;
  uint32_t level;
  uint32_t run;
};

class Filter {
 private:
  class FilterImpl;
  const std::unique_ptr<FilterImpl> impl_;

 public:
  /**
   * @brief Construct a handler for an _existing_ Bloom Filter. See
   * `docs/file_filter.md` for format. This constructor will NOT overwrite files
   * in the filesystem, the other one will!
   *
   * @param id The ID of the filter. There is one unique one of these per filter
   * file in the DB.
   * @param buf The buffer pool of pages from disk.
   * @param seed An initial randomization seed.
   */
  Filter(const FilterId id, BufPool& buf, uint64_t seed);

  /**
   * @brief Construct a NEW Bloom Filter. See `docs/file_filter.md` for format.
   * This will create a new Bloom Filter in the filesystem, potentially
   * overwriting existing data! Please ensure that you intend this!
   *
   * @param id The ID of the filter. There is one unique one of these per filter
   * file in the DB.
   * @param buf A buffer pool cache for accessing pages in the filesystem.
   * @param seed A starting random seed for the hash functions in the
   * Blocked BloomFilter.
   * @param keys A large list of keys to write into the bloom filter with. There
   * are no other write methods on this class.
   */
  Filter(const FilterId id, BufPool& buf, uint64_t seed, std::vector<K> keys);
  ~Filter();

  /**
   * @brief Returns `true` if the filter MIGHT have the key, `false` if the
   * filter DEFINITELY does not have the key.
   */
  [[nodiscard]] bool Has(K key) const;
};
