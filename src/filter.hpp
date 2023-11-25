#pragma once

#include <cstdint>
#include <string>

#include "buf.hpp"
#include "constants.hpp"
#include "naming.hpp"

struct FilterId {
  uint32_t level;
  uint32_t run;
  uint32_t intermediate;
};

class Filter {
 private:
  class FilterImpl;
  const std::unique_ptr<FilterImpl> impl;

 public:
  /**
   * @brief Construct a new filter serializer object.
   *
   * @param buf A buffer pool cache for accessing pages in the filesystem.
   * @param seed A starting random seed for the hash functions in the
   * Blocked BloomFilter.
   */
  Filter(const DbNaming& dbname, BufPool& buf, uint64_t seed);
  ~Filter();

  /**
   * @brief Create a new filter file
   *
   * @param file The file to create
   * @param keys The (key, value) pairs going into the filter. In theory, only
   * keys need to be passed in, but during flushing of the Memtable we have
   * (key, values), meaning it's easy to just pass them in by reference.
   */
  void Create(std::string& filename, const std::vector<std::pair<K, V>>& keys);

  /**
   * @brief Returns `true` if the filter MIGHT have the key, `false` if the
   * filter DEFINITELY DOES NOT have the key.
   *
   * @param file The filter file to search.
   * @param key The key to query for.
   */
  [[nodiscard]] bool Has(std::string& filename, K key) const;
};
