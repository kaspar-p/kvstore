#include <cstdint>
#include <cstdlib>
#include <optional>

#include "constants.hpp"

class MinHeap {
 public:
  /**
   * @brief Construct a minheap to store the next key from a set of LSMRuns.

   */
   MinHeap(const std::vector<K>& initial_keys);
   ~MinHeap();

  /**
   * @brief
   * @param
   */
  bool IsEmpty();

  /**
   * @brief
   * @param
   */
  std::optional<std::pair<K, int>> Extract();

  /**
   * @brief Insert a key and extract the previous minumum
   *
   * @param key The key to insert
   * return
   */
  std::optional<std::pair<K, int>> InsertAndExtract(std::pair<K, int> next_pair);

 private:
  class MinHeapImpl;
  const std::unique_ptr<MinHeapImpl> impl;
};
