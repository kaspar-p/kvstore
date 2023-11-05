#include "filter.hpp"

#include <array>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "naming.hpp"
#include "xxhash.h"

constexpr static std::size_t kCacheLineBytes = 128;
constexpr static std::size_t kEntriesPerCacheLine =
    (kCacheLineBytes / kKeySize);
constexpr static std::size_t kFiltersPerPage = kPageSize / kCacheLineBytes;
constexpr static std::size_t kFilterBits = kCacheLineBytes * 8;
constexpr static std::size_t kBitsPerEntry = 10;
constexpr static std::size_t kNumHashFuncs = 7;  // log(2) * kBitsPerEntry;

using HashFn = std::function<uint32_t(K)>;
using BloomFilter = std::bitset<kFilterBits>;

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
  const std::array<HashFn, kNumHashFuncs> bit_hashes;
  const uint32_t max_entries;
  const uint32_t num_filters;
  const uint64_t seed;
  const std::string filename;
  BufPool& buf;
  std::fstream file;

  [[nodiscard]] bool bloom_has(const BloomFilter& filter, const K key) const {
    bool val = true;
    for (const auto& fn : this->bit_hashes) {
      val = filter.test(fn(key) % kFilterBits);

      // If the bloom filter returns a 0 for any of the values, this is the
      // DEFINITE_NO answer
      if (!val) {
        return false;
      }
    }

    // If all bits are set, this is the MAYBE_YES answer
    return true;
  }

  Buffer to_buf(std::array<BloomFilter, kFiltersPerPage> filters) const {
    Buffer buffer;
    for (int i = 0; i < filters.size(); i++) {
      uint64_t data = htonll(filters.at(i).to_ullong());
      int factor = sizeof(uint64_t) / sizeof(std::byte);
      for (int j = 0; j < factor; j++) {
        uint8_t octet = (data >> (j * 8));
        buffer.at(i * factor + j) = std::byte{octet};
      }
    }

    return buffer;
  }

  std::array<BloomFilter, kFiltersPerPage> from_buf(Buffer buffer) const {
    std::array<BloomFilter, kFiltersPerPage> filters;
    for (int i = 0; i < kFiltersPerPage; i++) {
      uint64_t data = 0;
      int factor = sizeof(uint64_t) / sizeof(std::byte);
      for (int j = 0; j < factor; j++) {
        data |= static_cast<uint8_t>(buffer.at(i * factor + j)) << (j * 8);
      }
      BloomFilter filter(data);
      filters.at(i) = filter;
    }
    return filters;
  }

 public:
  FilterImpl(std::string dbname, uint32_t level, uint32_t max_elems,
             BufPool& buf, uint64_t starting_seed)
      : max_entries(max_elems),
        num_filters(this->max_entries / kEntriesPerCacheLine),
        seed(starting_seed),
        buf(buf),
        bit_hashes(create_hash_funcs(starting_seed)),
        filename(filter_file(dbname, level)) {
    assert(this->max_entries % kEntriesPerCacheLine == 0);
    std::uint32_t num_blocks = this->max_entries / kEntriesPerCacheLine;

    this->file.open(this->filename,
                    std::ios::binary | std::ios::out | std::ios::in);
  }

  [[nodiscard]] bool Has(K key) {
    uint64_t global_filter_idx =
        block_hash(key, this->seed) % this->num_filters;
    uint32_t page_idx = global_filter_idx / kPageSize;
    uint64_t page_filter_idx = global_filter_idx % kPageSize;

    const BloomFilter filter;

    // Attempt to read the page out of the buffer
    PageId page_id = PageId{.filename = this->filename, .page = page_idx};
    std::optional<BufferedPage> buf_page = buf.GetPage(page_id);
    if (buf_page.has_value()) {
      auto filters = this->from_buf(buf_page.value().contents);
      return this->bloom_has(filters.at(page_filter_idx), key);
    } else {
      // Or read it ourselves.
      this->file.seekg(page_idx * kPageSize);
      // std::array<BloomFilter, kFiltersPerPage> filters;
      Buffer buf;
      assert(this->file.good());
      this->file.read((char*)&buf, kPageSize);
      assert(this->file.good());

      // Put the page back in the buffer
      this->buf.PutPage(page_id, PageType::kFilters, buf);

      std::array<BloomFilter, kFiltersPerPage> filters = this->from_buf(buf);
      return this->bloom_has(filters.at(page_filter_idx), key);
    }
  }

  /**
   * @brief Puts the key into the set.
   */
  void Put(K key) {
    uint64_t global_block_idx = block_hash(key, this->seed) % this->num_filters;
    uint64_t page = global_block_idx / kPageSize;
    uint64_t page_block_idx = global_block_idx % kPageSize;

    this->file.seekg(global_block_idx * kCacheLineBytes);
    uint64_t block_data;

    assert(this->file.good());
    this->file.read(reinterpret_cast<char*>(block_data), kCacheLineBytes);
    assert(this->file.good());

    BloomFilter block(block_data);

    for (const auto& fn : this->bit_hashes) {
      block.set(fn(key) % kFilterBits);
    }

    this->file.seekp(global_block_idx * kCacheLineBytes);
    this->file.write(reinterpret_cast<char*>(block.to_ullong()),
                     sizeof(unsigned long long));
  }
};

Filter::Filter(std::string dbname, uint32_t level, uint32_t max_elems,
               BufPool& buf, uint64_t starting_seed)
    : impl_(std::make_unique<FilterImpl>(dbname, level, max_elems, buf,
                                         starting_seed)) {}
Filter::~Filter() = default;

bool Filter::Has(K key) const { return this->impl_->Has(key); }

void Filter::Put(K key) { return this->impl_->Put(key); }