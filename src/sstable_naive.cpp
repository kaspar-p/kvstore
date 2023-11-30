#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "fileutil.hpp"
#include "sstable.hpp"

SstableNaive::SstableNaive(BufPool& buffer_pool) : buffer_pool(buffer_pool){};

K SstableNaive::GetMinimum(std::string& filename) const {
  std::fstream file(
      filename, std::fstream::binary | std::fstream::in | std::fstream::out);
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  assert(file.good());
  std::array<uint64_t, 5> buf{};
  file.read(reinterpret_cast<char*>(buf.data()), 5 * sizeof(uint64_t));
  assert(file.good());

  return buf.at(3);
}

K SstableNaive::GetMaximum(std::string& filename) const {
  std::fstream file(
      filename, std::fstream::binary | std::fstream::in | std::fstream::out);
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  assert(file.good());
  std::array<uint64_t, 5> buf{};
  file.read(reinterpret_cast<char*>(buf.data()), 5 * sizeof(uint64_t));
  assert(file.good());

  return buf.at(4);
}

std::vector<std::pair<K, V>> SstableNaive::Drain(std::string& filename) const {
  return this->ScanInFile(filename, 0, UINT64_MAX);
}

void SstableNaive::Flush(std::string& filename,
                         std::vector<std::pair<K, V>>& pairs,
                         bool truncate) const {
  std::fstream::openmode mode =
      std::fstream::binary | std::fstream::in | std::fstream::out;
  if (truncate) {
    mode |= std::fstream::trunc;
  }
  std::fstream file(filename, mode);

  constexpr int kPageKeys = kPageSize / sizeof(uint64_t);
  std::array<uint64_t, kPageKeys> metadata_buf{};
  put_magic_numbers(metadata_buf, FileType::kData);

  // Insert the metadata.
  metadata_buf.at(2) = pairs.size();
  metadata_buf.at(3) = pairs.front().first;
  metadata_buf.at(4) = pairs.back().first;

  assert(file.is_open());
  assert(file.good());
  file.seekp(0);
  file.write(reinterpret_cast<char*>(metadata_buf.data()), kPageSize);
  assert(file.good());

  std::vector<uint64_t> data_buf;
  std::size_t padding = kPageKeys - ((2 * pairs.size()) % kPageKeys);
  data_buf.resize(2 * pairs.size() + padding);

  // Insert the key value pairs
  for (std::size_t i = 0; i < pairs.size(); i++) {
    data_buf.at((2 * i) + 0) = pairs.at(i).first;
    data_buf.at((2 * i) + 1) = pairs.at(i).second;
  }

  assert(file.good());
  file.seekp(kPageSize);
  file.write(reinterpret_cast<char*>(data_buf.data()),
             data_buf.size() * sizeof(uint64_t));
  assert(file.good());

  file.flush();
  assert(file.good());
};

struct BinarySearchResult {
  V val;
  uint64_t key_index;
};

constexpr int kPairSize = 2;

std::optional<BinarySearchResult> binary_search(std::fstream& file,
                                                std::size_t elems, K key,
                                                bool get_closest_bound) {
  int left = 0;
  int right = elems * kPairSize;
  int mid = left + floor((right - left) / 4) * 2;
  while (left <= right) {
    mid = left + floor((right - left) / 4) * 2;

    // Read the page that `mid` is in
    // TODO(kfp) use BufPool!

    // first page is metadata page
    assert(file.good());
    uint64_t page_idx = ((mid * sizeof(uint64_t)) / kPageSize) + 1;
    file.seekg(static_cast<std::streamoff>(page_idx * kPageSize));
    std::array<uint64_t, kPageSize / sizeof(uint64_t)> mid_page{};
    assert(file.good());
    file.read(reinterpret_cast<char*>(mid_page.data()), kPageSize);
    assert(file.good());

    uint64_t mid_in_page = mid % (kPageSize / sizeof(uint64_t));
    K found_key = mid_page.at(mid_in_page);
    if (found_key == key || (get_closest_bound && left == right)) {
      return std::make_optional(BinarySearchResult{
          .val = mid_page.at(mid_in_page + 1),
          .key_index = static_cast<uint64_t>(mid),
      });
    }

    if (found_key < key) {
      left = mid + kPairSize;
    } else {
      right = mid - kPairSize;
    }
  }

  if (get_closest_bound) {
    return BinarySearchResult{.val = key,
                              .key_index = static_cast<uint64_t>(mid)};
  }

  return std::nullopt;
}

std::optional<V> SstableNaive::GetFromFile(std::string& filename,
                                           const K key) const {
  std::fstream file(
      filename, std::fstream::binary | std::fstream::in | std::fstream::out);
  std::array<uint64_t, kPageSize / sizeof(uint64_t)> metadata{};
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(metadata.data()), kPageSize);
  assert(file.good());

  if (!has_magic_numbers(metadata, FileType::kData)) {
    std::cout << "Magic number wrong! Expected " << file_magic() << " but got "
              << metadata[0] << '\n';
    exit(1);
  }

  // If there are no elements
  uint64_t elems = metadata.at(2);
  if (elems == 0) {
    return std::nullopt;
  }

  // If the key is out of bounds for the page
  K lower = metadata.at(3);
  K upper = metadata.at(4);
  if (key < lower || key > upper) {
    return std::nullopt;
  }

  std::optional<BinarySearchResult> res =
      binary_search(file, elems, key, false);
  if (res.has_value()) {
    return res.value().val;
  }
  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableNaive::ScanInFile(std::string& filename,
                                                      const K lower,
                                                      const K upper) const {
  assert(lower <= upper);
  std::fstream file(
      filename, std::fstream::binary | std::fstream::in | std::fstream::out);

  std::array<uint64_t, kPageSize / sizeof(uint64_t)> metadata_page{};
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(metadata_page.data()), kPageSize);
  assert(file.good());

  if (!has_magic_numbers(metadata_page, FileType::kData)) {
    std::cout << "Magic number wrong! Expected " << file_magic() << " but got "
              << metadata_page.at(0) << '\n';
    exit(1);
  }

  std::vector<std::pair<K, V>> l;

  // If there are no elements
  uint64_t elems = metadata_page.at(2);
  if (elems == 0) {
    return l;
  }

  // If the keys are out of bounds for the page
  K page_lower = metadata_page.at(3);
  K page_upper = metadata_page.at(4);
  if ((upper < page_lower) || (lower > page_upper)) {
    return l;
  }

  std::optional<BinarySearchResult> res =
      binary_search(file, elems, lower, true);
  assert(res.has_value());

  std::array<uint64_t, kPageSize / sizeof(uint64_t)> data_buf;

  int pages_read = 0;
  assert(file.good());
  file.seekg(((res.value().key_index / kPageSize) * (kPageSize)) + kPageSize);
  file.read(reinterpret_cast<char*>(data_buf.data()), kPageSize);
  assert(file.good());

  // Walk along the file, collecting key value pairs
  uint64_t walk = res.value().key_index % (kPageSize / sizeof(uint64_t));
  uint64_t original_page_walk =
      res.value().key_index % (kPageSize / sizeof(uint64_t));

  constexpr std::size_t kPageKeys = kPageSize / sizeof(uint64_t);

  // Keep walking until:
  // 1. the key is out of bounds, or
  // 2. the walking variable is greater than the number of elements
  while (data_buf.at(walk) <= upper &&
         walk + (pages_read * kPageKeys) < elems * kPairSize) {
    l.emplace_back(data_buf.at(walk), data_buf.at(walk + 1));
    walk += 2;

    // If we have walked an entire page already, read the next page
    if (walk - original_page_walk == kPageKeys) {
      walk = 0;
      original_page_walk = 0;
      pages_read++;

      assert(file.good());
      file.read(reinterpret_cast<char*>(data_buf.data()), kPageSize);
      assert(file.good());
    }
  }

  return l;
}
