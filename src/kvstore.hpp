#pragma once

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"

class DatabaseClosedException : public std::exception {
 public:
  [[nodiscard]] const char* what() const noexcept override;
};

class FailedToOpenException : public std::exception {
 public:
  [[nodiscard]] const char* what() const noexcept override;
};

class OnlyTheDatabaseCanUseFunnyValuesException : public std::exception {
 public:
  [[nodiscard]] const char* what() const noexcept override;
};

class DatabaseInUseException : public std::exception {
 public:
  [[nodiscard]] const char* what() const noexcept override;
};

enum DataFileFormat { kBTree, kFlatSorted };

struct Options {
  /**
   * @brief The data directory to create the database in.
   *
   * Defaults to "./", the current directory
   */
  std::optional<std::filesystem::path> dir;

  /**
   * @brief The number of elements to buffer in-memory before flushing to the
   * filesystem.
   *
   * Fewer elements flush more often. This leads to better data volatility, but
   * also might impact performance.
   *
   * Defaults to roughly 1MB worth of elements.
   */
  std::optional<std::size_t> memory_buffer_elements;

  /**
   * @brief The initial amount of memory to allocate towards the page buffer.
   * Pages are usually around 4KB, though the metadata to allocate that is very
   * small. Must be <= buffer_pages_maximum.
   *
   * Defaults to 16.
   */
  std::optional<std::size_t> buffer_pages_initial;

  /**
   * @brief The maximum number of filesystem pages to buffer. They are usually
   * around 4KB. After this limit is reached, pages are evicted to make space
   * for new pages. Must be >= buffer_pages_initial.
   *
   * Defaults to 128
   */
  std::optional<std::size_t> buffer_pages_maximum;

  /**
   * @brief The number of runs within an LSM-level. This also is the fan-out
   * factor that the database grows by, meaning level L will have `tiers` times
   * more capacity than level L-1.
   *
   * @example If level 1 can store 2GB, and `tiers == 4`, then level 2 can store
   * 8GB of data.
   *
   * Defaults to 2.
   */
  std::optional<std::uint8_t> tiers;

  /**
   * @brief The way to serialize the data files. Note that the serialization
   * methods are not compatible with each other, and databases opened with one
   * file format should not be opened with a future format, it will likely
   * break.
   *
   * Likely this option shouldn't be touched, as FlatSorted is slower than BTree
   * anyway. Only really useful for perf testing.
   *
   * Defaults to DataFileFormat::BTree
   */
  std::optional<DataFileFormat> serialization;
};

class KvStore {
 private:
  class KvStoreImpl;
  std::unique_ptr<KvStoreImpl> impl;

 public:
  KvStore();
  ~KvStore();

  /**
   * @brief Opens a database, either new or used.
   * The data directory is where all persistent data files
   * for the key-value store will be. Permissions issues
   * will throw errors.
   *
   * See `Close()` for closing.
   *
   * @param name A unique database name, also the name of the directory where
   * the data is stored.
   */
  void Open(const std::string& name, Options options);

  /**
   * @brief Returns the path to the data directory, if needed.
   * If the database has not been Open()ed, throws a DatabaseClosedException
   * exception.
   *
   * @return std::filesystem::path
   */
  [[nodiscard]] std::filesystem::path DataDirectory() const;

  /**
   * @brief Paired operation with `Open()`, closes the
   * database and cleans up resources.
   */
  void Close();

  /**
   * @brief Get a single value from the database by its key. Returns
   * std::nullopt if the key doesn't exist in the database.
   *
   * @param key The key to search for.
   * @return V* A pointer to the value, and std::nullptr if there was no such
   * value
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
   * @brief Put a (key, value) pair into the database. If the key already
   * exists, overwrites the value.
   *
   * @param key The key to insert.
   * @param value The value to insert.
   */
  void Put(K key, V value);

  /**
   * @brief Delete a (key, value) pair from the database. Does nothing if the
   * key does not exist.
   *
   * @param key The key to delete
   */
  void Delete(K key);
};