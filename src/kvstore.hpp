#pragma once

#include <exception>
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

class OnlyTheDatabaseCanUseFunnyValues : public std::exception {
 public:
  [[nodiscard]] const char* what() const noexcept override;
};

struct Options {
  bool overwrite;
};

class KvStore {
 private:
  class KvStoreImpl;
  std::unique_ptr<KvStoreImpl> impl_;

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
   * @brief Paired operation with `Open()`, closes the
   * database and cleans up resources.
   */
  void Close();

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
   * @brief Get a single value from the database by its key. Returns
   * std::nullopt if the key doesn't exist in the database.
   *
   * @param key The key to search for.
   * @return V* A pointer to the value, and std::nullptr if there was no such
   * value
   */
  [[nodiscard]] std::optional<V> Get(K key) const;

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