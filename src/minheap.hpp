#pragma once

#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

class MinHeap {
 public:
  /**
   * @brief Construct a minheap to store the next key from a set of LSMRuns.
   * @param initial_keys The initial keys to add to the minheap.
   * Assumes each key's index in the initial_keys vector is equal to the index
   * of that key's run in its LSMLevel.
   */
  explicit MinHeap(const std::vector<K>& initial_keys);
  ~MinHeap();

  /**
   * @brief
   * @param
   */
  bool IsEmpty() const;

  /**
   * @brief
   * @param
   */
  std::optional<std::pair<K, std::size_t>> Extract();

  std::size_t Size() const;

  /**
   * @brief Insert a key and extract the previous minumum
   *
   * @param key The key to insert
   * return
   */
  std::optional<std::pair<K, std::size_t>> InsertAndExtract(
      std::pair<K, std::size_t> next_pair);

  /**
   * @brief Insert a key
   *
   * @param key The key to insert
   * return
   */
  void Insert(std::pair<K, std::size_t> next_pair);


 private:
  class MinHeapImpl;
  const std::unique_ptr<MinHeapImpl> impl;
};
