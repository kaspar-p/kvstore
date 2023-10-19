#pragma once

#include <functional>
#include <iostream>
#include <string>

#include "memtable.hpp"

typedef uint64_t K;
typedef uint64_t V;

class Sstable
{
public:
  Sstable() noexcept {};

  /**
   * @brief Create a new file in the data directory with name @param filename
   * from an entire @param memtable. Assumes that the file doesn't already
   * exist. The structure of the file is an implementation detail, and comes
   * from either the `sst_btree.cpp` or `sst_naive.cpp`.
   *
   * @param filename The name of the file to create
   * @param memtable The MemTable to flush.
   */
  virtual void flush(std::string filename, MemTable& memtable);

  /**
   * @brief Get a value from a file, or std::nullopt if it doesn't exist.
   *
   * @param file The file to get from
   * @param key The key to search for
   * @return std::optional<V> The resulting value.
   */
  virtual std::optional<V> get_from_file(std::fstream file, const K key);

  /**
   * @brief Scan keys in range [lower, upper] from @param file.
   *
   * @param file The file to scan in
   * @param lower The lower bound of the scan
   * @param upper The upper bound of the scan
   * @return std::vector<std::pair<K, V>>
   */
  virtual std::vector<std::pair<K, V>> scan_in_file(std::fstream file,
                                                    const K lower,
                                                    const K upper);
};
