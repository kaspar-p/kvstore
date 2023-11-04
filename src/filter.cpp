#include "filter.hpp"

#include <array>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>

#include "constants.hpp"
#include "xxhash.h"

constexpr static std::size_t kCacheLineBytes = 128;
constexpr static std::size_t kEntriesPerCacheLine =
    (kCacheLineBytes / kKeySize);
constexpr static std::size_t kBlockBits = kEntriesPerCacheLine * 8;
constexpr static std::size_t kBitsPerEntry = 10;
constexpr static std::size_t kNumHashFuncs = 7;  // log(2) * kBitsPerEntry;

using HashFn = std::function<uint32_t(K)>;

std::array<HashFn, kNumHashFuncs> create_hash_funcs(uint64_t starting_seed) {
  std::array<HashFn, kNumHashFuncs> fns;
  for (int i = 0; i < kNumHashFuncs; i++) {
    fns.at(i) = [starting_seed, i](K key) {
      return static_cast<uint64_t>(
          XXH64(&key, kKeySize, (i + 1) + starting_seed + 1));
    };
  }
  return fns;
}

[[nodiscard]] uint64_t block_hash(K key, uint64_t starting_seed) {
  return static_cast<uint64_t>(XXH64(&key, kKeySize, starting_seed));
}

class Filter::FilterImpl {
 private:
  const std::array<HashFn, kNumHashFuncs> bit_hashes_;
  const uint32_t max_entries_;
  const uint64_t seed;

  std::vector<std::bitset<kBlockBits>> blocks_;

 public:
  explicit FilterImpl(uint32_t max_elems)
      : max_entries_(max_elems), seed(0), bit_hashes_(create_hash_funcs(0)) {
    assert(this->max_entries_ % kEntriesPerCacheLine == 0);
    std::uint32_t num_blocks = this->max_entries_ / kEntriesPerCacheLine;

    this->blocks_.resize(num_blocks);
    for (int i = 0; i < num_blocks; i++) {
      this->blocks_.at(i) &= 0;
    }
  }

  FilterImpl(uint32_t max_elems, uint64_t starting_seed)
      : max_entries_(max_elems),
        seed(starting_seed),
        bit_hashes_(create_hash_funcs(starting_seed)) {
    assert(this->max_entries_ % kEntriesPerCacheLine == 0);
    std::uint32_t num_blocks = this->max_entries_ / kEntriesPerCacheLine;

    this->blocks_.resize(num_blocks);
    for (int i = 0; i < num_blocks; i++) {
      this->blocks_.at(i) &= 0;
    }
  }

  [[nodiscard]] bool Has(K key) const {
    uint64_t block_idx = block_hash(key, this->seed) % this->blocks_.size();
    const auto& bloom_block = this->blocks_.at(block_idx);

    bool val = true;
    for (const auto& fn : this->bit_hashes_) {
      val = bloom_block.test(fn(key) % kBlockBits);

      // If the bloom filter returns a 0 for any of the values, this is the
      // DEFINITE_NO answer
      if (!val) {
        return false;
      }
    }

    // If all bits are set, this is the MAYBE_YES answer
    return true;
  }

  /**
   * @brief Puts the key into the set.
   */
  void Put(K key) {
    uint64_t block_idx = block_hash(key, this->seed) % this->blocks_.size();
    auto& bloom_block = this->blocks_.at(block_idx);

    for (const auto& fn : this->bit_hashes_) {
      bloom_block.set(fn(key) % kBlockBits);
    }
  }
};

Filter::Filter(uint32_t max_elems, uint64_t starting_seed)
    : impl_(std::make_unique<FilterImpl>(max_elems, starting_seed)) {}
Filter::Filter(uint32_t max_elems)
    : impl_(std::make_unique<FilterImpl>(max_elems)) {}
Filter::~Filter() = default;

bool Filter::Has(K key) const { return this->impl_->Has(key); }

void Filter::Put(K key) { return this->impl_->Put(key); }