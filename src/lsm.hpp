#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "naming.hpp"

class LSMLevel {
 private:
  class LSMLevelImpl;
  const std::unique_ptr<LSMLevelImpl> impl_;

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
           std::size_t memory_buffer_size, BufPool& buf);
  ~LSMLevel();

  /**
   * @brief Get the level
   */
  uint32_t Level() const;

  /**
   * @brief Create a new file within the level. It doesn't contain data yet,
   * it's returned to get filled with data.
   */
  std::fstream CreateFile();

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

  /**
   * @brief Merge two LSMLevel's with the same `level`
   *
   * @param level The other level to merge with. Assumed to have the same
   * level value as this LSMLevel.
   * @return LSMLevel A new level, with level one greater than the two merged.
   */
  LSMLevel MergeWith(LSMLevel level);
};
