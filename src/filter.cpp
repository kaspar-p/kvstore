#include "filter.hpp"

#include <any>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <string>

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
      if (key == 600) {
        std::cout << "TESTING BIT: " << fn(key) % kFilterBits << '\n';
      }
      val = bloom_test(filter, fn(key) % kFilterBits);

      // If the bloom filter returns a 0 for any of the values, this is the
      // DEFINITE_NO answer
      if (!val) {
        return false;
      }
    }

    // If all bits are set, this is the MAYBE_YES answer
    return true;
  }

  [[nodiscard]] bool static bloom_test(const BloomFilter& filter,
                                       uint32_t bit_offset) {
    constexpr int kBitsInByte = 8;
    uint32_t byte = bit_offset / kBitsInByte;
    uint32_t bit = bit_offset % kBitsInByte;
    uint32_t bit_val = (filter.at(byte) >> bit) & 1;
    assert(bit_val == 1 || bit_val == 0);
    return bit_val == 1;
  };

  void static bloom_set(BloomFilter& filter, uint32_t bit_offset) {
    constexpr int kBitsInByte = 8;
    uint32_t byte = bit_offset / kBitsInByte;
    uint32_t bit = bit_offset % kBitsInByte;

    filter.at(byte) = filter.at(byte) | (1 << bit);
  };

  [[nodiscard]] std::array<KeyHashFn, kNumHashFuncs> static create_hash_funcs(
      uint64_t starting_seed) {
    std::array<KeyHashFn, kNumHashFuncs> fns;
    for (std::size_t i = 0; i < kNumHashFuncs; i++) {
      fns.at(i) = [starting_seed, i](K key) {
        return static_cast<uint64_t>(
            XXH64(&key, kKeySize, (i + 1) + starting_seed + 1));
      };
    }
    return fns;
  }

  [[nodiscard]] uint64_t static block_hash(K key, uint64_t starting_seed) {
    return static_cast<uint64_t>(XXH64(&key, kKeySize, starting_seed));
  }

  BytePage static to_buf(std::array<BloomFilter, kFiltersPerPage>& filters) {
    BytePage buffer{};
    for (std::size_t filt = 0; filt < filters.size(); filt++) {
      std::array<uint8_t, kFilterBytes> data_vec = filters.at(filt);
      for (std::size_t data_offset = 0; data_offset < data_vec.size();
           data_offset++) {
        std::byte data = std::byte{data_vec.at(data_offset)};
        buffer.at(filt * kFilterBytes + data_offset) = data;
      }
    }

    return buffer;
  }

  std::array<BloomFilter, kFiltersPerPage> static from_buf(BytePage& buffer) {
    std::array<BloomFilter, kFiltersPerPage> filters;
    for (std::size_t i = 0; i < kFiltersPerPage; i++) {
      std::array<uint8_t, kFilterBytes> filter_buf;
      for (std::size_t data_offset = 0; data_offset < kFilterBytes;
           data_offset++) {
        filter_buf.at(data_offset) =
            static_cast<uint8_t>(buffer.at(i * kFilterBytes + data_offset));
      }

      filters.at(i) = filter_buf;
    }
    return filters;
  }

  uint64_t static num_filters(uint64_t num_entries) {
    if (num_entries == 0) {
      return 0;
    }
    return ceil(static_cast<float>(num_entries) / kEntriesPerFilter);
  }

  void write_metadata_block(std::fstream& file, uint64_t num_entries) {
    assert(file.good());
    std::array<uint64_t, kPageSize / sizeof(uint64_t)> metadata_block{};
    put_magic_numbers(metadata_block, FileType::kFilter);
    metadata_block.at(kNumEntries) = num_entries;

    file.write(reinterpret_cast<char*>(metadata_block.data()), kPageSize);
    assert(file.good());

    uint64_t n_filters = num_filters(num_entries);
    for (uint64_t filter_idx = 0; filter_idx < n_filters; filter_idx++) {
      std::array<uint8_t, kFilterBytes> zeroes{};
      file.write(reinterpret_cast<char*>(zeroes.data()), kFilterBytes);
    }

    uint64_t pad = kPageSize - ((n_filters % kFiltersPerPage) * kFilterBytes);
    std::vector<uint8_t> buf_pad{};
    buf_pad.resize(pad);

    file.write(reinterpret_cast<char*>(buf_pad.data()), buf_pad.size());
    assert(file.good());
  }

  void batch_write_keys(std::fstream& file,
                        const std::vector<std::pair<K, V>>& pairs) {
    std::vector<BloomFilter> filters{};

    uint64_t n_filters = num_filters(pairs.size());
    filters.resize(n_filters);

    for (auto const& pair : pairs) {
      K key = pair.first;
      uint64_t filter_idx = block_hash(key, this->seed) % n_filters;

      BloomFilter& filter = filters.at(filter_idx);
      for (const auto& fn : this->bit_hashes) {
        if (key == 600) {
          std::cout << "SETTING BIT: " << fn(key) % kFilterBits << " in filter "
                    << filter_idx << '\n';
        }
        bloom_set(filter, fn(key) % kFilterBits);
      }
    }

    std::vector<BytePage> pages{};
    pages.resize(ceil(static_cast<float>(n_filters) / kFiltersPerPage));

    std::size_t filter_idx = 0;
    for (std::size_t page_idx = 0; page_idx < pages.size(); page_idx++) {
      std::array<BloomFilter, kFiltersPerPage> page_filters{};
      std::size_t batch = 0;
      while (batch < kFiltersPerPage && filter_idx < n_filters) {
        page_filters.at(batch) = filters.at(filter_idx);
        filter_idx++;
        batch++;
      }

      pages.at(page_idx) = this->to_buf(page_filters);
    }

    // Write the file data
    assert(file.good());
    file.seekp(1 * kPageSize);  // Leave room for metadata block
    for (std::size_t page = 0; page < pages.size(); page++) {
      file.write(reinterpret_cast<char*>(pages.at(page).data()), kPageSize);
    }
    assert(file.good());
  }

 public:
  FilterImpl(const DbNaming& dbname, BufPool& buf, const uint64_t starting_seed)
      : dbname(dbname),
        seed(starting_seed),
        bit_hashes(create_hash_funcs(starting_seed)),
        buf(buf) {}

  void Create(std::string& filename,
              const std::vector<std::pair<K, V>>& pairs) {
    std::fstream file(filename, std::fstream::binary | std::fstream::out |
                                    std::fstream::in | std::fstream::trunc);
    assert(file.is_open());
    assert(file.good());

    if (pairs.front().first == 200 && pairs.front().second == kTombstoneValue) {
      std::cout << "Creating filter " << filename << ", pairs:" << '\n';
      std::cout << "Entries: " << pairs.size()
                << ", and num_filters: " << num_filters(pairs.size()) << '\n';
      for (auto& pair : pairs) {
        std::cout << pair.first << "," << pair.second << '\n';
      }
    }

    this->write_metadata_block(file, pairs.size());
    this->batch_write_keys(file, pairs);
  }

  [[nodiscard]] bool Has(std::string& filename, K key) {
    std::fstream file(
        filename, std::fstream::binary | std::fstream::in | std::fstream::out);
    assert(file.is_open());
    assert(file.good());

    std::array<uint64_t, kPageSize / sizeof(uint64_t)> metadata_page{};
    file.seekg(0);
    file.read(reinterpret_cast<char*>(metadata_page.data()), kPageSize);

    assert(has_magic_numbers(metadata_page, FileType::kFilter));

    // Calculate filter and bit offsets
    uint64_t num_elements = metadata_page.at(2);
    if (num_elements == 0) return false;

    uint64_t n_filters = num_filters(num_elements);

    uint64_t global_filter_idx = block_hash(key, this->seed) % n_filters;
    uint32_t page_idx = calc_page_idx(global_filter_idx);
    uint64_t filter_offset = calc_page_offset(global_filter_idx);

    if (key == 600) {
      std::cout << "chose filter " << global_filter_idx << " in page "
                << page_idx << '\n';
    }

    // Attempt to read the page out of the buffer
    PageId page_id = PageId{.filename = filename, .page = page_idx};
    std::optional<BufferedPage> buf_page = buf.GetPage(page_id);
    if (buf_page.has_value()) {
      if (key == 600) {
        std::cout << "was in buf!" << '\n';
      }
      auto buf = std::any_cast<BytePage>(buf_page.value().contents);
      auto filters = this->from_buf(buf);
      return bloom_has(filters.at(filter_offset), key);
    } else {
      // Or read it ourselves.
      file.seekg(page_idx * kPageSize);
      BytePage buf{};
      assert(file.good());
      file.read(reinterpret_cast<char*>(buf.data()), kPageSize);
      assert(file.good());

      std::cout << "PUTTING PAGE IN BUFFER: " << page_id.filename << ","
                << page_id.page << '\n';

      // Put the page back in the buffer
      this->buf.PutPage(page_id, std::make_any<BytePage>(buf));

      std::array<BloomFilter, kFiltersPerPage> filters = this->from_buf(buf);

      return bloom_has(filters.at(filter_offset), key);
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