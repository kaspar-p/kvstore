#include "lsm.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <memory>

#include "filter.hpp"
#include "naming.hpp"

class LSMLevel::LSMLevelImpl {
 private:
  const uint64_t max_entries;
  const uint32_t level;
  const std::size_t memory_buffer_size;
  const bool is_final;
  const DbNaming& dbname;

  BufPool& buf;
  std::optional<Filter> filter;

 public:
  LSMLevelImpl(const DbNaming& dbname, uint32_t level, bool is_final,
               std::size_t memory_buffer_size, BufPool& buf)
      : max_entries(pow(2, level) * memory_buffer_size),
        level(level),
        memory_buffer_size(memory_buffer_size),
        is_final(is_final),
        dbname(dbname),
        buf(buf) {}
  ~LSMLevelImpl() = default;
  uint32_t Level() const { return this->level; }

  [[nodiscard]] std::optional<V> Get(K key) const {
    if (this->filter.has_value() && !this->filter.value().Has(key)) {
      return std::nullopt;
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const {
    return {};
  }

  LSMLevel MergeWith(LSMLevel level) {
    assert(level.Level() == this->Level());

    // TODO(kfp): do an actual merging algorithm
    return LSMLevel(this->dbname, this->level + 1, this->is_final,
                    this->memory_buffer_size, this->buf);
  };
};

LSMLevel::LSMLevel(const DbNaming& dbname, int level, bool is_final,
                   std::size_t memory_buffer_size, BufPool& Buf)
    : impl_(std::make_unique<LSMLevelImpl>(dbname, level, is_final,
                                           memory_buffer_size, Buf)) {}

LSMLevel::~LSMLevel() = default;

uint32_t LSMLevel::Level() const { return this->impl_->Level(); }
std::optional<V> LSMLevel::Get(K key) const { return this->impl_->Get(key); }
std::vector<std::pair<K, V>> LSMLevel::Scan(K lower, K upper) const {
  return this->impl_->Scan(lower, upper);
};