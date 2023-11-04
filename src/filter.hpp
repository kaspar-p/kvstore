#pragma once

#include <cstdint>
#include <memory>

#include "constants.hpp"

class Filter
{
private:
  class FilterImpl;
  const std::unique_ptr<FilterImpl> impl_;

public:
  explicit Filter(uint32_t max_elems);
  Filter(uint32_t max_elems, uint64_t seed);
  ~Filter();

  /**
   * @brief Returns `true` if the filter MIGHT have the key, `false` if the
   * filter DEFINITELY does not have the key
   */
  [[nodiscard]] bool Has(K key) const;

  /**
   * @brief Puts the key into the set.
   */
  void Put(K key);
};
