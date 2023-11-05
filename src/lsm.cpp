#include "lsm.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <memory>

#include "filter.hpp"

class LSMLevel::LSMLevelImpl {
 private:
  const uint64_t max_entries_;
  const uint32_t level_;
  const std::size_t memory_buffer_size_;
  const bool is_final_;
  const std::string dbname;

  BufPool& buf;
  Filter filter_;

 public:
  LSMLevelImpl(std::string dbname, uint32_t level, bool is_final,
               std::size_t memory_buffer_size, BufPool& buf)
      : max_entries_(pow(2, level) * memory_buffer_size),
        level_(level),
        memory_buffer_size_(memory_buffer_size),
        is_final_(is_final),
        dbname(dbname),
        buf(buf),
        filter_(dbname, level, this->max_entries_, buf, 0) {}
  ~LSMLevelImpl() = default;
  uint32_t Level() const { return this->level_; }

  [[nodiscard]] std::optional<V> Get(K key) const {
    if (!this->filter_.Has(key)) {
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
    return LSMLevel(this->dbname, this->level_ + 1, this->is_final_,
                    this->memory_buffer_size_, this->buf);
  };
};

LSMLevel::LSMLevel(std::string dbname, int level, bool is_final,
                   std::size_t memory_buffer_size, BufPool& Buf)
    : impl_(std::make_unique<LSMLevelImpl>(dbname, level, is_final,
                                           memory_buffer_size, Buf)) {}

LSMLevel::~LSMLevel() = default;

uint32_t LSMLevel::Level() const { return this->impl_->Level(); }
std::optional<V> LSMLevel::Get(K key) const { return this->impl_->Get(key); }
std::vector<std::pair<K, V>> LSMLevel::Scan(K lower, K upper) const {
  return this->impl_->Scan(lower, upper);
};