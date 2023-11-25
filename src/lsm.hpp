#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "manifest.hpp"
#include "naming.hpp"
#include "sstable.hpp"

class LSMRun {
 public:
  /**
   * @brief Construct a new LSMRun manager instance. Meant to manage a single
   * run within a single LSMLevel.
   *
   * @param naming The database naming scheme.
   * @param level The level that the run is a part of, 0-indexed.
   * @param run The run within the level, 0-indexed, where run 0 is the "first
   * run" in the level, not that there is any real ordering between runs anyway.
   * @param tiers The number of maximum runs in a level. Used to calculate the
   * number of files that this run is allowed to manage.
   */
  LSMRun(const DbNaming& naming, int level, int run, uint8_t tiers,
         std::size_t memtable_capacity, Manifest& manifest, BufPool& buf,
         Sstable& sstable_serializer, Filter& filter_serializer);
  ~LSMRun();

  /**
   * @brief Get a single value from the level by its key. Returns
   * std::nullopt if the key doesn't exist in the level.
   *
   * @param key The key to search for.
   * @return V The value, and std::nullopt if there was no such
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

  /**
   * @brief Discover the files already in the filesystem.
   *
   * Meant to be used by the LSMLevel class on initialization or recovery.
   */
  void DiscoverFiles();

  /**
   * @brief Returns the index of the next file. That is, if the run has one file
   * in it already, this method will return `1`, the first file being at index
   * 0.
   *
   * Intended to be used during compaction and flushing the memtable.
   */
  [[nodiscard]] int NextFile() const;

  /**
   * @brief Meant to be used during compaction (run creation), register a new
   * file to be managed by that run.
   *
   * @param filename The file to register into the run.
   */
  void RegisterNewFile(int intermediate);

 private:
  class LSMRunImpl;
  std::unique_ptr<LSMRunImpl> impl;
};

class LSMLevel {
 public:
  /**
   * @brief Create a new LSM level manager
   *
   * @param level The level, where 0 is the memtable, and 1 is the first file.
   * @param is_final Important for Dostoevsky, where the final level is merged
   * through levelling, but all others are merged through tiering.
   * @param memory_buffer_size The size of the memtable, or level 0. Each level
   * is 2x the size of the previous level.
   */
  LSMLevel(const DbNaming& dbname, int level, bool is_final,
           std::size_t memory_buffer_size, Manifest& manifest, BufPool& buf);
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

  // TODO(kfp): make this return a run for the next level?
  void RegisterNewRun(std::unique_ptr<LSMRun> run);

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

  //  *
  //  * @param level The other level to merge with. Assumed to have the same
  //  * level value as this LSMLevel.
  //  * @return LSMLevel A new level, with level one greater than the two
  //  merged.
  //  */
  // [[nodiscard]] LSMLevel MergeWith(LSMLevel level);

 private:
  class LSMLevelImpl;
  const std::unique_ptr<LSMLevelImpl> impl;
};
