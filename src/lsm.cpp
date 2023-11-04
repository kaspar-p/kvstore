#include <cassert>
#include <cmath>
#include <cstdlib>
#include <memory>

#include "lsm.hpp"

#include "filter.hpp"

class LSMLevel::LSMLevelImpl
{
private:
  const uint64_t max_entries_;
  const uint32_t level_;
  const std::size_t memory_buffer_size_;
  const bool is_final_;

  Filter filter_;

public:
  LSMLevelImpl(uint32_t level, bool is_final, std::size_t memory_buffer_size)
      : max_entries_(pow(2, level) * memory_buffer_size)
      , level_(level)
      , memory_buffer_size_(memory_buffer_size)
      , is_final_(is_final)
      , filter_(this->max_entries_)
  {
  }

  ~LSMLevelImpl() = default;
  uint32_t Level() const { return this->level_; }

  [[nodiscard]] std::optional<V> Get(K key) const
  {
    if (!this->filter_.Has(key)) {
      return std::nullopt;
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const
  {
    return {};
  }

  LSMLevel MergeWith(LSMLevel level)
  {
    assert(level.Level() == this->Level());

    // TODO(kfp): do an actual merging algorithm
    return LSMLevel(
        this->level_ + 1, this->is_final_, this->memory_buffer_size_);
  };
};

LSMLevel::LSMLevel(int level, bool is_final, std::size_t memory_buffer_size)
    : impl_(std::make_unique<LSMLevelImpl>(level, is_final, memory_buffer_size))
{
}

LSMLevel::~LSMLevel() = default;

uint32_t LSMLevel::Level() const
{
  return this->impl_->Level();
}
