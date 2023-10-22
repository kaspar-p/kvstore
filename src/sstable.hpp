#pragma once

#include <fstream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "memtable.hpp"

class Sstable
{
public:
  /**
   * @brief Create a new file in the data directory with name @param filename
   * from an entire @param memtable. Assumes that the file doesn't already
   * exist. The structure of the file is an implementation detail, and comes
   * from either the `sst_btree.cpp` or `sst_naive.cpp`.
   *
   * @param filename The name of the file to create
   * @param memtable The MemTable to flush.
   */
  virtual void Flush(std::string filename, MemTable& memtable);

  /**
   * @brief Get a value from a file, or std::nullopt if it doesn't exist.
   *
   * @param file The file to get from
   * @param key The key to search for
   * @return std::optional<V> The resulting value.
   */
  virtual std::optional<V> GetFromFile(std::fstream& file, K key);

  /**
   * @brief Scan keys in range [lower, upper] from @param file.
   *
   * @param file The file to scan in
   * @param lower The lower bound of the scan
   * @param upper The upper bound of the scan
   * @return std::vector<std::pair<K, V>>
   */
  virtual std::vector<std::pair<K, V>> ScanInFile(std::fstream& file,
                                                  K lower,
                                                  K upper);
};

class SstableNaive
{
public:
  static void Flush(std::fstream& filename, MemTable& memtable);
  static std::optional<V> GetFromFile(std::fstream& file, K key);
  static std::vector<std::pair<K, V>> ScanInFile(std::fstream& file,
                                                 K lower,
                                                 K upper);
};

class SstableBTree
{
public:
  static void Flush(std::fstream& filename, MemTable& memtable);
  static std::optional<V> GetFromFile(std::fstream& file, K key);
  static std::vector<std::pair<K, V>> ScanInFile(std::fstream& file,
                                                 K lower,
                                                 K upper);
};