#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "memtable.hpp"

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
  virtual void Flush(
      std::fstream& filename,
      std::unique_ptr<std::vector<std::pair<K, V>>> pairs) const = 0;

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
};

class SstableNaive : public Sstable {
 public:
  SstableNaive();
  void Flush(
      std::fstream& filename,
      std::unique_ptr<std::vector<std::pair<K, V>>> pairs) const override;
  std::optional<V> GetFromFile(std::fstream& file, K key) const override;
  std::vector<std::pair<K, V>> ScanInFile(std::fstream& file, K lower,
                                          K upper) const override;
};

class SstableBTree : public Sstable {
 public:
  SstableBTree();
  void Flush(
      std::fstream& filename,
      std::unique_ptr<std::vector<std::pair<K, V>>> pairs) const override;
  std::optional<V> GetFromFile(std::fstream& file, K key) const override;
  std::vector<std::pair<K, V>> ScanInFile(std::fstream& file, K lower,
                                          K upper) const override;
};
