#pragma once

#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

/**
 * @brief Merge a vector of sorted buffers. The semantics are: The buffers that
 * come LATER (have a higher index) have precedence. For example, if buffers at
 * index 0 and index 1 contain all the same keys, it is as if the buffer at
 * index 1 is copied into the result vector.
 *
 * @param sorted_buffers
 * @return std::vector<std::pair<K, V>>
 */
std::vector<std::pair<K, V>> minheap_merge(
    std::vector<std::vector<std::pair<K, V>>>& sorted_buffers);

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
  [[nodiscard]] std::optional<std::pair<K, std::size_t>> Extract();

  [[nodiscard]] std::size_t Size() const;

  /**
   * @brief Insert a key and extract the previous minumum
   *
   * @param key The key to insert
   * return
   */
  [[nodiscard]] std::optional<std::pair<K, std::size_t>> InsertAndExtract(
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
