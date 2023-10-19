#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

typedef uint64_t K;
typedef uint64_t V;
#define PAGE_SIZE (4096)

class MemTableFullException : public std::exception
{
public:
  char* what();
};

class MemTable
{
private:
  class MemTableImpl;
  std::unique_ptr<MemTableImpl> pimpl;

public:
  /**
   * @brief Constructs a MemTable object, the maximum size given by the
   * parameter. All inserts beyond that size will throw an error, see the
   * `Put()` method for details.
   *
   * @param memtable_size The size, in elements, of the memtable.
   */
  MemTable(unsigned long long memtable_size);
  ~MemTable();
  MemTable(const MemTable&);
  MemTable& operator=(const MemTable&);
  MemTable(MemTable&& t) = default;
  MemTable& operator=(MemTable&& t) = default;

  /**
   * @brief Returns a string representation of the tree, meant only for
   * visualization purposes.
   *
   * @return std::string
   */
  std::string Print() const;

  /**
   * @brief Get a value by key. Returns nullptr if does not exist.
   *
   * @param key The key to search for.
   * @return V* A pointer to the value, or nullptr.
   */
  V* Get(const K key) const;

  /**
   * @brief Put a value into the tree. Replaces the value if the key was
   * previously in the tree. If the value was present, return it back out. If
   * not, returns `std::nullopt`.
   *
   * If the table is full (see MemTable(int) constructor), throws a
   * `MemTableFullException`.
   *
   * @param key The key to insert.
   * @param value The value to insert.
   * @return std::optional<V> The old value, if it existed.
   */
  std::optional<V> Put(const K key, const V value);

  /**
   * @brief Get a vector of sorted (key, value) pairs, where all keys k are such
   * that lower <= k <= upper. The ranges do not have to actually be keys, and
   * do not have to be within the range of actual keys.
   *
   * @param lower The lower bound, inclusive.
   * @param upper THe upper bound, inclusive.
   * @return std::vector<std::pair<K, V>> A list of ordered pairs, with lowest
   * key earliest.
   */
  std::vector<std::pair<K, V>> Scan(const K lower, const K upper) const;

  /**
   * @brief Get a list of all pairs in the MemTable. Same as Scan() with [-inf,
   * +inf] bounds.
   *
   * @return std::vector<std::pair<K, V>> All pairs in the table.
   */
  std::vector<std::pair<K, V>> ScanAll() const;

  /**
   * @brief Deletes a key from the table. Returns a nullptr if the key was never
   * there, or a pointer to the old value if it is.
   *
   * @param key The key to delete
   * @return V* A pointer to the old value, or a nullptr
   */
  V* Delete(const K key);

  /**
   * @brief Removes _all_ elements from the tree, leaving it empty!
   */
  void Clear();
};
