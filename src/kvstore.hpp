#pragma once

#include "memtable.hpp"

typedef unsigned long long int K;
typedef unsigned long long int V;

class KvStore
{
private:
  std::unique_ptr<MemTable> memtable;
  bool open;
  std::string name;

public:
  KvStore(void);

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
  void Open(const std::string name);

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
  std::vector<std::pair<K, V>> Scan(const K lower, const K upper) const;

  /**
   * @brief Get a single value from the database by its key. Returns a
   * _non-owning_ pointer to the value, and std::nullptr if there is no such key
   * in the database.
   *
   * @param key The key to search for.
   * @return V* A pointer to the value, and std::nullptr if there was no such
   * value
   */
  V* Get(const K key) const;

  /**
   * @brief Put a (key, value) pair into the database. If the key already
   * exists, overwrites the value.
   *
   * @param key The key to insert.
   * @param value The value to insert.
   */
  void Put(const K key, const V value);

  /**
   * @brief Delete a (key, value) pair from the database. Does nothing if the
   * key does not exist.
   *
   * @param key The key to delete
   */
  void Delete(const K key);
};