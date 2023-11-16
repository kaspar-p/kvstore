#include "filter.hpp"

#include <any>
#include <array>
#include <bitset>
#include <cassert>
#include <cmath>
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

using KeyHashFn = std::function<uint32_t(K)>;

constexpr static std::size_t kCacheLineBytes = 128;
constexpr static std::size_t kFilterBytes = 128;
static_assert(kCacheLineBytes >= kFilterBytes);
constexpr static std::size_t kFilterBits = kFilterBytes * 8;

constexpr static std::size_t kBitsPerEntry = 5;
constexpr static std::size_t kEntriesPerFilter = (kFilterBits / kBitsPerEntry);
constexpr static std::size_t kFiltersPerPage = kPageSize / kFilterBytes;
constexpr static std::size_t kNumHashFuncs = 3;  // log(2) * kBitsPerEntry;

using BloomFilter = std::array<uint8_t, kFilterBytes>;

enum FilterFileLocations {
  kNumEntries = 2,
};

uint64_t calc_page_idx(uint64_t global_filter_idx) {
  uint64_t page_idx = global_filter_idx / kFiltersPerPage;
  return page_idx + 1;  // First page is always metadata page
}

uint64_t calc_page_offset(uint64_t global_filter_idx) {
  return global_filter_idx % kFiltersPerPage;
};

class Filter::FilterImpl {
 private:
  BufPool& buf;

  const std::string filename;
  std::fstream file;

  const std::array<KeyHashFn, kNumHashFuncs> bit_hashes;
  const uint64_t seed;
  const uint32_t num_filters;

  [[nodiscard]] bool bloom_has(const BloomFilter& filter, const K key) const {
    bool val = true;
    for (const auto& fn : this->bit_hashes) {
      val = this->bloom_test(filter, fn(key) % kFilterBits);

      // If the bloom filter returns a 0 for any of the values, this is the
      // DEFINITE_NO answer
      if (!val) {
        return false;
      }
    }

    // If all bits are set, this is the MAYBE_YES answer
    return true;
  }

  [[nodiscard]] bool bloom_test(const BloomFilter& filter,
                                uint32_t bit_offset) const {
    uint32_t byte = bit_offset / 8;
    uint32_t bit = bit_offset % 8;
    uint32_t bit_val = (filter.at(byte) >> bit) & 1;
    assert(bit_val == 1 || bit_val == 0);
    return bit_val == 1;
  };

  void bloom_set(BloomFilter& filter, uint32_t bit_offset) {
    uint32_t byte = bit_offset / 8;
    uint32_t bit = bit_offset % 8;

    filter.at(byte) = filter.at(byte) | (1 << bit);
  };

  std::array<KeyHashFn, kNumHashFuncs> create_hash_funcs(
      uint64_t starting_seed) const {
    std::array<KeyHashFn, kNumHashFuncs> fns;
    for (int i = 0; i < kNumHashFuncs; i++) {
      fns.at(i) = [starting_seed, i](K key) {
        return static_cast<uint64_t>(
            XXH64(&key, kKeySize, (i + 1) + starting_seed + 1));
      };
    }
    return fns;
  }

  [[nodiscard]] uint64_t block_hash(K key, uint64_t starting_seed) const {
    return static_cast<uint64_t>(XXH64(&key, kKeySize, starting_seed));
  }

  BytePage to_buf(std::array<BloomFilter, kFiltersPerPage>& filters) const {
    BytePage buffer{};
    for (int filt = 0; filt < filters.size(); filt++) {
      std::array<uint8_t, kFilterBytes> data_vec = filters.at(filt);
      for (int data_offset = 0; data_offset < data_vec.size(); data_offset++) {
        std::byte data = std::byte{data_vec.at(data_offset)};
        buffer.at(filt * kFilterBytes + data_offset) = data;
      }
    }

    return buffer;
  }

  std::array<BloomFilter, kFiltersPerPage> from_buf(BytePage& buffer) const {
    std::array<BloomFilter, kFiltersPerPage> filters;
    for (int i = 0; i < kFiltersPerPage; i++) {
      std::array<uint8_t, kFilterBytes> filter_buf;
      for (int data_offset = 0; data_offset < kFilterBytes; data_offset++) {
        filter_buf.at(data_offset) =
            static_cast<uint8_t>(buffer.at(i * kFilterBytes + data_offset));
      }

      filters.at(i) = filter_buf;
    }
    return filters;
  }

  bool file_exists() {
    // Read the file, if the magic number exists then it's already there
    // If not, write it entirely new.
    if (std::filesystem::exists(this->filename) &&
        std::filesystem::file_size(this->filename) > kPageSize) {
      // If there is at least a page in the file, we assume it's correct
      uint64_t firstPage[kPageSize / sizeof(uint64_t)];
      if (!this->file.is_open()) {
        this->file.open(this->filename, std::fstream::binary |
                                            std::fstream::out |
                                            std::fstream::in);
      }
      assert(this->file.good());
      this->file.seekg(0);
      this->file.read(reinterpret_cast<char*>(firstPage), kPageSize);
      bool has_magic = has_magic_numbers(firstPage, FileType::kFilter);
      if (has_magic) {
        return true;
      } else {
        // overwrite the file, it's likely garbage
        return false;
      }
    }
    return false;
  }

  void write_metadata_block() {
    assert(this->file.good());
    uint64_t metadata_block[kPageSize / sizeof(uint64_t)]{};
    put_magic_numbers(metadata_block, FileType::kFilter);
    metadata_block[kNumEntries] = this->num_filters * kEntriesPerFilter;

    this->file.write(reinterpret_cast<char*>(metadata_block), kPageSize);
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

  void batch_write_keys(std::vector<K> keys) {
    std::vector<BloomFilter> filters{};
    filters.resize(this->num_filters);

    for (auto const& key : keys) {
      uint64_t filter_idx = block_hash(key, this->seed) % this->num_filters;
      BloomFilter& filter = filters.at(filter_idx);
      for (const auto& fn : this->bit_hashes) {
        bloom_set(filter, fn(key) % kFilterBits);
      }
    }

    std::vector<BytePage> pages{};

    int filter_idx = 0;
    while (filter_idx < this->num_filters) {
      std::array<BloomFilter, kFiltersPerPage> page_filters;
      for (int batch = 0; batch < kFiltersPerPage; batch++) {
        page_filters.at(batch) = filters.at(filter_idx);
      }
      pages.push_back(this->to_buf(page_filters));
      filter_idx += kFiltersPerPage;
    }

    // Write the file data
    assert(this->file.good());
    this->file.seekp(1 * kPageSize);  // Leave room for metadata block
    for (int page = 0; page < pages.size(); page++) {
      this->file.write(reinterpret_cast<char*>(pages.at(page).data()),
                       kPageSize);
    }
    assert(this->file.good());
  }

  uint32_t find_num_filters() {
    std::cout << "before" << '\n';
    if (this->file_exists()) {
      std::cout << "here" << '\n';
      uint64_t metadata_buf[kPageSize / sizeof(uint64_t)];
      this->file.seekg(0);
      this->file.read(reinterpret_cast<char*>(metadata_buf), kPageSize);

      uint64_t max_entries = metadata_buf[2];
      return max_entries / kEntriesPerFilter;
    }
    return 0;
  }

 public:
  FilterImpl(const DbNaming& dbname, const FilterId id, BufPool& buf,
             const uint64_t starting_seed)
      : filename(filter_file(dbname, id.level, id.run, id.intermediate)),
        num_filters(find_num_filters()),
        seed(starting_seed),
        buf(buf),
        bit_hashes(create_hash_funcs(starting_seed)) {
    assert(num_filters > 0);
  }

  FilterImpl(const DbNaming& dbname, const FilterId id, BufPool& buf,
             const uint64_t starting_seed, const std::vector<K> keys)
      : filename(filter_file(dbname, id.level, id.run, id.intermediate)),
        num_filters(ceil((float)keys.size() / kEntriesPerFilter)),
        seed(starting_seed),
        buf(buf),
        bit_hashes(create_hash_funcs(starting_seed)) {
    if (!this->file.is_open()) {
      this->file.open(this->filename, std::fstream::binary | std::fstream::out |
                                          std::fstream::in);
    }

    // If all keys are there, nothing to write
    // There are no false negatives, so this search is fine.
    if (this->file_exists()) {
      bool has_all = true;
      for (const auto& key : keys) {
        if (!this->Has(key)) {
          return;
        }
      }
    }

    this->file = std::fstream(this->filename,
                              std::fstream::binary | std::fstream::out |
                                  std::fstream::in | std::fstream::trunc);
    assert(this->file.is_open());
    this->write_metadata_block();
    this->batch_write_keys(keys);
  }

  [[nodiscard]] bool Has(K key) {
    if (this->num_filters == 0) {
      return false;
    }

    uint64_t global_filter_idx =
        block_hash(key, this->seed) % this->num_filters;
    uint32_t page_idx = calc_page_idx(global_filter_idx);
    uint64_t filter_offset = calc_page_offset(global_filter_idx);

    const BloomFilter filter{};

    // Attempt to read the page out of the buffer
    PageId page_id = PageId{.filename = this->filename, .page = page_idx};
    std::optional<BufferedPage> buf_page = buf.GetPage(page_id);
    if (buf_page.has_value()) {
      auto buf = std::any_cast<BytePage>(buf_page.value().contents);
      auto filters = this->from_buf(buf);
      return this->bloom_has(filters.at(filter_offset), key);
    } else {
      // Or read it ourselves.
      this->file.seekg(page_idx * kPageSize);
      BytePage buf{};
      assert(this->file.good());
      this->file.read(reinterpret_cast<char*>(buf.data()), kPageSize);
      assert(this->file.good());

      // Put the page back in the buffer
      this->buf.PutPage(page_id, std::make_any<BytePage>(buf));

      std::array<BloomFilter, kFiltersPerPage> filters = this->from_buf(buf);

      return this->bloom_has(filters.at(filter_offset), key);
    }
  }
};

Filter::Filter(const DbNaming& dbname, const FilterId id, BufPool& buf,
               const uint64_t seed, const std::vector<K> keys)
    : impl_(std::make_unique<FilterImpl>(dbname, id, buf, seed, keys)) {}

Filter::Filter(const DbNaming& dbname, const FilterId id, BufPool& buf,
               const uint64_t seed)
    : impl_(std::make_unique<FilterImpl>(dbname, id, buf, seed)) {}

Filter::~Filter() = default;

bool Filter::Has(K key) const { return this->impl_->Has(key); }