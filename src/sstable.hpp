#pragma once

#include <cstdint>
#include <fstream>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

struct SstableId {
  uint32_t level;
  uint32_t run;
  uint32_t intermediate;
};

class Sstable {
 public:
  virtual ~Sstable() = default;

  /**
   * @brief Create a new file in the data directory with name @param filename
   * from an entire @param memtable. Assumes that the file doesn't already
   * exist. The structure of the file is an implementation detail, and comes
   * from either the `sst_btree.cpp` or `sst_naive.cpp`.
   *
   * @param filename The name of the file to create
   * @param memtable The MemTable to flush.
   */
  virtual void Flush(std::fstream& filename,
                     std::vector<std::pair<K, V>>& pairs) const = 0;

  /**
   * @brief Get a value from a file, or std::nullopt if it doesn't exist.
   *
   * @param file The file to get from
   * @param key The key to search for
   * @return std::optional<V> The resulting value.
   */
  virtual std::optional<V> GetFromFile(std::fstream& file, K key) const = 0;

  /**
   * @brief Scan keys in range [lower, upper] from @param file.
   *
   * @param file The file to scan in
   * @param lower The lower bound of the scan
   * @param upper The upper bound of the scan
   * @return std::vector<std::pair<K, V>>
   */
  virtual std::vector<std::pair<K, V>> ScanInFile(std::fstream& file, K lower,
                                                  K upper) const = 0;

  /**
   * @brief Get the minimum key in the file. Assumes file is a datafile.
   */
  virtual K GetMinimum(std::fstream& file) const = 0;

  /**
   * @brief Get the maximum key in the file. Assumes file is a datafile.
   */
  virtual K GetMaximum(std::fstream& file) const = 0;

  /**
   * @brief Drain the file into a vector of key-value pairs.
   */
  virtual std::vector<std::pair<K,V>> Drain() const = 0;
};

class SstableNaive : public Sstable {
 public:
  SstableNaive();
  void Flush(std::fstream& filename,
             std::vector<std::pair<K, V>>& pairs) const override;
  std::optional<V> GetFromFile(std::fstream& file, K key) const override;
  std::vector<std::pair<K, V>> ScanInFile(std::fstream& file, K lower,
                                          K upper) const override;
  K GetMinimum(std::fstream& file) const override;
  K GetMaximum(std::fstream& file) const override;
  std::vector<std::pair<K,V>> Drain() const override;
};

class SstableBTree : public Sstable {
 public:
  SstableBTree();
  void Flush(std::fstream& filename,
             std::vector<std::pair<K, V>>& pairs) const override;
  std::optional<V> GetFromFile(std::fstream& file, K key) const override;
  std::vector<std::pair<K, V>> ScanInFile(std::fstream& file, K lower,
                                          K upper) const override;
  K GetMinimum(std::fstream& file) const override;
  K GetMaximum(std::fstream& file) const override;
  std::vector<std::pair<K,V>> Drain() const override;
};
