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
  const DbNaming& dbname;
  const uint64_t seed;
  const std::array<KeyHashFn, kNumHashFuncs> bit_hashes;
  BufPool& buf;

  [[nodiscard]] bool bloom_has(const BloomFilter& filter, const K key) const {
    bool val = true;
    for (const auto& fn : this->bit_hashes) {
      val = this->bloom_test(filter, fn(key) % kFilterBits);
      std::cout << "testing bit " << fn(key) % kFilterBits << " for key " << key
                << " ::: " << val << std::endl;

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

  uint64_t num_filters(uint64_t num_entries) {
    if (num_entries == 0) {
      return 0;
    }
    return ceil((float)num_entries / kEntriesPerFilter);
  }

  void write_metadata_block(std::fstream& file, uint64_t num_entries) {
    assert(file.good());
    uint64_t metadata_block[kPageSize / sizeof(uint64_t)]{};
    put_magic_numbers(metadata_block, FileType::kFilter);
    metadata_block[kNumEntries] = num_entries;

    file.write(reinterpret_cast<char*>(metadata_block), kPageSize);
    assert(file.good());

    uint64_t num_filters = this->num_filters(num_entries);
    for (int filter_idx = 0; filter_idx < num_filters; filter_idx++) {
      int page = (filter_idx * kFilterBytes) / kPageSize;

      char zeroes[kFilterBytes]{};
      file.write(zeroes, kFilterBytes);
    }

    int pad = kPageSize - ((num_filters % kFiltersPerPage) * kFilterBytes);
    char buf_pad[pad];
    std::memset(buf_pad, 0, pad);

    file.write(buf_pad, pad);
    assert(file.good());
  }

  void batch_write_keys(std::fstream& file,
                        std::vector<std::pair<K, V>> pairs) {
    std::vector<BloomFilter> filters{};

    uint64_t num_filters = this->num_filters(pairs.size());
    filters.resize(num_filters);

    for (auto const& pair : pairs) {
      K key = pair.first;
      uint64_t filter_idx = block_hash(key, this->seed) % num_filters;

      BloomFilter& filter = filters.at(filter_idx);
      for (const auto& fn : this->bit_hashes) {
        std::cout << "setting bit " << fn(key) % kFilterBits << " for key "
                  << key << " in filter " << filter_idx << std::endl;
        bloom_set(filter, fn(key) % kFilterBits);
      }
    }

    std::vector<BytePage> pages{};

    int filter_idx = 0;
    while (filter_idx < num_filters) {
      std::array<BloomFilter, kFiltersPerPage> page_filters;
      for (int batch = 0; batch < kFiltersPerPage; batch++) {
        page_filters.at(batch) = filters.at(filter_idx);
      }
      pages.push_back(this->to_buf(page_filters));
      filter_idx += kFiltersPerPage;
    }

    // Write the file data
    assert(file.good());
    file.seekp(1 * kPageSize);  // Leave room for metadata block
    for (int page = 0; page < pages.size(); page++) {
      file.write(reinterpret_cast<char*>(pages.at(page).data()), kPageSize);
    }
    assert(file.good());
  }

 public:
  FilterImpl(const DbNaming& dbname, BufPool& buf, const uint64_t starting_seed)
      : dbname(dbname),
        seed(starting_seed),
        buf(buf),
        bit_hashes(create_hash_funcs(starting_seed)) {}

  void Create(std::string& filename, std::vector<std::pair<K, V>> pairs) {
    std::fstream file(filename, std::fstream::binary | std::fstream::out |
                                    std::fstream::in | std::fstream::trunc);
    assert(file.is_open());
    assert(file.good());

    std::cout << "num_filters " << num_filters(pairs.size()) << '\n';

    this->write_metadata_block(file, pairs.size());
    this->batch_write_keys(file, pairs);
  }

  [[nodiscard]] bool Has(std::string& filename, K key) {
    std::fstream file(
        filename, std::fstream::binary | std::fstream::in | std::fstream::out);
    assert(file.is_open());
    assert(file.good());

    uint64_t metadata_page[kPageSize / sizeof(uint64_t)];
    file.seekg(0);
    file.read(reinterpret_cast<char*>(metadata_page), kPageSize);

    assert(has_magic_numbers(metadata_page, FileType::kFilter));

    // Calculate filter and bit offsets
    uint64_t num_elements = metadata_page[2];
    if (num_elements == 0) return false;

    uint64_t num_filters = this->num_filters(num_elements);

    std::cout << "num_filters " << num_filters << '\n';

    uint64_t global_filter_idx = block_hash(key, this->seed) % num_filters;
    std::cout << "checking filter " << global_filter_idx << " for key " << key
              << '\n';
    uint32_t page_idx = calc_page_idx(global_filter_idx);
    uint64_t filter_offset = calc_page_offset(global_filter_idx);

    std::cout << "in page " << page_idx << " within page at index "
              << filter_offset << std::endl;

    // Attempt to read the page out of the buffer
    PageId page_id = PageId{.filename = filename, .page = page_idx};
    std::optional<BufferedPage> buf_page = buf.GetPage(page_id);
    if (buf_page.has_value()) {
      auto buf = std::any_cast<BytePage>(buf_page.value().contents);
      auto filters = this->from_buf(buf);
      return this->bloom_has(filters.at(filter_offset), key);
    } else {
      // Or read it ourselves.
      file.seekg(page_idx * kPageSize);
      BytePage buf{};
      assert(file.good());
      file.read(reinterpret_cast<char*>(buf.data()), kPageSize);
      assert(file.good());

      // Put the page back in the buffer
      this->buf.PutPage(page_id, std::make_any<BytePage>(buf));

      std::array<BloomFilter, kFiltersPerPage> filters = this->from_buf(buf);

      return this->bloom_has(filters.at(filter_offset), key);
    }

    file.close();
  }
};

Filter::Filter(const DbNaming& dbname, BufPool& buf, const uint64_t seed)
    : impl(std::make_unique<FilterImpl>(dbname, buf, seed)) {}
Filter::~Filter() = default;

void Filter::Create(std::string& file,
                    const std::vector<std::pair<K, V>>& keys) {
  return this->impl->Create(file, keys);
}
[[nodiscard]] bool Filter::Has(std::string& filename, K key) const {
  return this->impl->Has(filename, key);
}