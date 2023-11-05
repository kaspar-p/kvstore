#include "filter.hpp"

#include <array>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "fileutil.hpp"
#include "naming.hpp"
#include "xxhash.h"

constexpr static std::size_t kCacheLineBytes = 128;
constexpr static std::size_t kFilterBytes = 128;
static_assert(kCacheLineBytes >= kFilterBytes);

constexpr static std::size_t kDataTypePerFilter =
    kFilterBytes / sizeof(unsigned long long);
constexpr static std::size_t kEntriesPerFilter = (kFilterBytes / kKeySize);
constexpr static std::size_t kFiltersPerPage = kPageSize / kFilterBytes;
constexpr static std::size_t kFilterBits = kFilterBytes * 8;
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

uint64_t calc_page_idx(uint64_t global_page_idx) {
  uint64_t page_idx = global_page_idx / kPageSize;
  return page_idx + 1;  // First page is always metadata page
}

uint64_t calc_page_offset(uint64_t global_page_idx) {
  return global_page_idx % kPageSize;
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

  std::array<uint8_t, kFilterBytes> ser_filter(BloomFilter& filter) const {
    std::array<uint8_t, kFilterBytes> data{};
    for (int i = 0; i < kFilterBytes; i++) {
      uint8_t byte = 0;
      for (int j = 0; j < 8; j++) {
        std::size_t idx = 8 * i + j;
        bool bit = filter.test((8 * i) + j);
        byte = byte | (bit << (7 - j));
      }
      data.at(i) = byte;
    }
    return data;
  }

  BloomFilter de_filter(std::array<uint8_t, kFilterBytes>& data) const {
    BloomFilter filter;
    for (int i = 0; i < kFilterBytes; i++) {
      uint8_t byte = data.at(i);
      for (int j = 0; j < 8; j++) {
        std::size_t idx = 8 * i + j;
        bool bit = (byte << j) >> 7;
        filter.set(idx, bit);
      }
    }
    return filter;
  }

  Buffer to_buf(std::array<BloomFilter, kFiltersPerPage>& filters) const {
    Buffer buffer;
    for (int filt = 0; filt < filters.size(); filt++) {
      std::array<uint8_t, kFilterBytes> data_vec =
          this->ser_filter(filters.at(filt));
      for (int data_offset = 0; data_offset < data_vec.size(); data_offset++) {
        std::byte data = std::byte{data_vec.at(data_offset)};
        buffer.at(filt * kFilterBytes + data_offset) = data;
      }
    }

    return buffer;
  }

  std::array<BloomFilter, kFiltersPerPage> from_buf(Buffer& buffer) const {
    std::array<BloomFilter, kFiltersPerPage> filters;
    for (int i = 0; i < kFiltersPerPage; i++) {
      std::array<uint8_t, kFilterBytes> filter_buf;
      for (int data_offset = 0; data_offset < kFilterBytes; data_offset++) {
        filter_buf.at(data_offset) =
            static_cast<uint8_t>(buffer.at(i * kFilterBytes + data_offset));
      }

      filters.at(i) = this->de_filter(filter_buf);
    }
    return filters;
  }

  void initialize_filter_file() {
    // Read the file, if the magic number exists then it's already there
    // If not, write it entirely new.
    if (std::filesystem::file_size(this->filename) > kPageSize) {
      // If there is at least a page in the file, we assume it's correct
      char firstPage[kPageSize];
      this->file.seekg(0);
      this->file.read(firstPage, kPageSize);
      bool has_magic = has_magic_numbers(firstPage, FileType::kFilter);
      if (has_magic) {
        return;
      } else {
        // overwrite the file, it's likely garbage
      }
    }

    assert(this->file.good());
    char metadata_block[kPageSize]{};
    put_magic_numbers(metadata_block, FileType::kFilter);
    this->file.write(metadata_block, kPageSize);
    assert(this->file.good());

    for (int filter_idx = 0; filter_idx < this->num_filters; filter_idx++) {
      int page = (filter_idx * kFilterBytes) / kPageSize;

      char zeroes[kFilterBytes]{};
      this->file.write(zeroes, kFilterBytes);
    }

    int pad =
        kPageSize - ((this->num_filters % kFiltersPerPage) * kFilterBytes);
    char buf_pad[pad];
    std::memset(buf_pad, 0, pad);

    this->file.write(buf_pad, pad);
    assert(this->file.good());
  }

 public:
  FilterImpl(const DbNaming& dbname, const uint32_t level,
             const uint32_t max_elems, BufPool& buf,
             const uint64_t starting_seed)
      : max_entries(max_elems),
        num_filters(this->max_entries / kEntriesPerFilter),
        seed(starting_seed),
        buf(buf),
        bit_hashes(create_hash_funcs(starting_seed)),
        filename(filter_file(dbname, level)) {
    assert(this->max_entries % kEntriesPerFilter == 0);

    this->file = std::fstream(this->filename,
                              std::fstream::binary | std::fstream::out |
                                  std::fstream::in | std::fstream::trunc);
    assert(this->file.is_open());
    this->initialize_filter_file();
  }

  [[nodiscard]] bool Has(K key) {
    uint64_t global_filter_idx =
        block_hash(key, this->seed) % this->num_filters;
    uint32_t page_idx = calc_page_idx(global_filter_idx);
    uint64_t page_offset = calc_page_offset(global_filter_idx);

    const BloomFilter filter;

    // Attempt to read the page out of the buffer
    PageId page_id = PageId{.filename = this->filename, .page = page_idx};
    std::optional<BufferedPage> buf_page = buf.GetPage(page_id);
    if (buf_page.has_value()) {
      auto filters = this->from_buf(buf_page.value().contents);
      return this->bloom_has(filters.at(page_offset), key);
    } else {
      // Or read it ourselves.
      // std::cout << "HAS: getting page ourselves\n";
      this->file.seekg(page_idx * kPageSize);
      char buf[kPageSize];
      assert(this->file.good());
      this->file.read(buf, kPageSize);
      assert(this->file.good());

      // Put the page back in the buffer
      Buffer byte_buf = FromRaw(buf);
      this->buf.PutPage(page_id, PageType::kFilters, byte_buf);

      std::array<BloomFilter, kFiltersPerPage> filters =
          this->from_buf(byte_buf);
      return this->bloom_has(filters.at(page_offset), key);
    }
  }

  /**
   * @brief Puts the key into the set.
   */
  void Put(K key) {
    uint64_t global_filter_idx =
        block_hash(key, this->seed) % this->num_filters;
    uint32_t page_idx = calc_page_idx(global_filter_idx);
    uint64_t page_offset = calc_page_offset(global_filter_idx);

    // Read the page out of buffer
    PageId page_id = PageId{.filename = this->filename, .page = page_idx};
    std::optional<BufferedPage> page = this->buf.GetPage(page_id);
    std::array<BloomFilter, kFiltersPerPage> filters{};
    if (page.has_value()) {
      Buffer byte_buf = page.value().contents;
      filters = this->from_buf(byte_buf);
    } else {
      // Or fetch it ourselves
      // std::cout << "PUT: getting page ourselves"
      // << "\n";
      char buf[kPageSize];
      this->file.seekg(page_idx * kPageSize);
      assert(this->file.is_open());
      assert(this->file.good());
      this->file.read(buf, kPageSize);
      assert(this->file.good());

      Buffer byte_buf = FromRaw(buf);
      filters = this->from_buf(byte_buf);
    }

    BloomFilter& filter = filters.at(page_offset);

    for (const auto& fn : this->bit_hashes) {
      filter.set(fn(key) % kFilterBits);
    }

    Buffer byte_buf = this->to_buf(filters);

    // Write the file data back
    assert(this->file.good());
    this->file.seekp(page_idx * kPageSize);
    char output[kPageSize];
    ToRaw(byte_buf, output);
    this->file.write(output, kPageSize);
    assert(this->file.good());

    // Put the buffer back in the cache
    this->buf.PutPage(page_id, PageType::kFilters, byte_buf);
  }
};

Filter::Filter(const DbNaming& dbname, uint32_t level, uint32_t max_elems,
               BufPool& buf, uint64_t starting_seed)
    : impl_(std::make_unique<FilterImpl>(dbname, level, max_elems, buf,
                                         starting_seed)) {}
Filter::~Filter() = default;

bool Filter::Has(K key) const { return this->impl_->Has(key); }

void Filter::Put(K key) { return this->impl_->Put(key); }