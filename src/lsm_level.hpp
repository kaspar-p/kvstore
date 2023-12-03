#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "lsm_run.hpp"
#include "manifest.hpp"
#include "naming.hpp"
#include "sstable.hpp"

class LSMLevel {
 public:
  /**
   * @brief Create a new LSM level manager
   *
   * @param level The level, where 0 is the memtable, and 1 is the first file.
   * @param is_final Important for Dostoevsky, where the final level is merged
   * through levelling, but all others are merged through tiering.
   * @param memtable_capacity The size of the memtable, or level 0. Each level
   * is 2x the size of the previous level.
   */
  LSMLevel(const DbNaming& dbname, uint8_t tiers, int level, bool is_final,
           std::size_t memtable_capacity, Manifest& manifest, BufPool& buf,
           Sstable& sstable_serializer);
  ~LSMLevel();

  /**
   * @brief Get the level
   */
  [[nodiscard]] uint32_t Level() const;

  /**
   * @brief Discover the runs that are in this database currently. Sets internal
   * variables. Meant to be used by the database class after instantiating new
   * LSMLevels, on startup.
   */
  void DiscoverRuns();

  std::optional<std::unique_ptr<LSMRun>> RegisterNewRun(
      std::unique_ptr<LSMRun> run,
      std::optional<std::reference_wrapper<LSMLevel>> next_level);

  /**
   * @brief Returns the index of the next run. That is, if the level has two
   * runs in it, this method returns `2`, as runs are 0-indexed.
   *
   * Intended to be used during compaction and flushing into the next level.
   */
  [[nodiscard]] int NextRun() const;

  /**
   * @brief Get a single value from the level by its key. Returns
   * std::nullopt if the key doesn't exist in the level.
   *
   * @param key The key to search for.
   * @return V The value, and std::nullptr if there was no such
   * value.
   */
  [[nodiscard]] std::optional<V> Get(K key) const;

  /**
   * @brief Get a vector of (key, value) pairs, sorted by key, where all keys k
   * are such that lower <= k <= upper. It is not required that `lower` or
   * `upper` be present within the database, nor that they be in range of
   * existing keys in the database.
   *
   * @param lower The lower bound of the scan search.
   * @param upper The upper bound of the scan search.
   * @return std::vector<std::pair<K, V>> An ordered list of (key, value) pairs.
   */
  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const;

 private:
  class LSMLevelImpl;
  const std::unique_ptr<LSMLevelImpl> impl;
};
