#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

SstableNaive::SstableNaive() {}

K SstableNaive::GetMinimum(std::fstream& file) const {
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  assert(file.good());
  uint64_t buf[4];
  file.read(reinterpret_cast<char*>(buf), 4 * sizeof(uint64_t));
  assert(file.good());

  return buf[2];
}

K SstableNaive::GetMaximum(std::fstream& file) const {
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  assert(file.good());
  uint64_t buf[4];
  file.read(reinterpret_cast<char*>(buf), 4 * sizeof(uint64_t));
  assert(file.good());

  return buf[3];
}

void SstableNaive::Flush(
    std::fstream& file,
    std::unique_ptr<std::vector<std::pair<K, V>>> pairs) const {
  constexpr int zeroes_in_page = kPageSize / sizeof(uint64_t);
  const std::size_t element_size = 4 + 2 * pairs->size();
  const std::size_t bufsize =
      element_size + (zeroes_in_page - (element_size % zeroes_in_page));
  uint64_t wbuf[bufsize];

  // Insert the metadata.
  wbuf[0] = 0x11223344;
  wbuf[1] = pairs->size();
  wbuf[2] = pairs->front().first;
  wbuf[3] = pairs->back().first;

  // Insert the key value pairs
  for (int i = 0; i < pairs->size(); i++) {
    wbuf[4 + (2 * i) + 0] = pairs->at(i).first;
    wbuf[4 + (2 * i) + 1] = pairs->at(i).second;
  }

  // Pad the rest of the page with zeroes
  for (int i = element_size; i < bufsize; i++) {
    wbuf[i] = 0;
  }

  assert(file.is_open());
  assert(file.good());
  file.seekp(0);
  for (uint64_t& elem : wbuf) {
    file.write(reinterpret_cast<char*>(&elem), sizeof(uint64_t));
    assert(file.good());
  }

  file.flush();
  assert(file.good());
};

struct BinarySearchResult {
  V val;
  uint64_t key_index;
};

std::optional<BinarySearchResult> binary_search(std::fstream& file, int elems,
                                                K key, bool get_closest_bound) {
  constexpr int header_size = 4;
  constexpr int pair_size = 2;

  uint64_t left = header_size;
  uint64_t right = header_size + elems * pair_size;
  uint64_t mid = left + floor((right - left) / 4) * 2;
  while (left <= right) {
    mid = left + floor((right - left) / 4) * 2;

    // Read the page that `mid` is in
    // TODO(kfp) use BufPool!
    uint64_t page_idx = (mid * sizeof(uint64_t)) / kPageSize;
    file.seekg(page_idx * kPageSize);
    uint64_t mid_page[kPageSize / sizeof(uint64_t)];
    file.read(reinterpret_cast<char*>(mid_page), kPageSize);

    uint64_t mid_in_page = mid % (kPageSize / sizeof(uint64_t));
    K found_key = mid_page[mid_in_page];
    if (found_key == key || (get_closest_bound && left == right)) {
      return std::make_optional(BinarySearchResult{
          .val = mid_page[mid_in_page + 1],
          .key_index = mid,
      });
    }

    if (found_key < key) {
      left = mid + pair_size;
    } else {
      right = mid - pair_size;
    }
  }

  if (get_closest_bound) {
    return BinarySearchResult{.val = key, .key_index = mid};
  }

  return std::nullopt;
}

std::optional<V> SstableNaive::GetFromFile(std::fstream& file,
                                           const K key) const {
  uint64_t metadata[kPageSize];
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(metadata), kPageSize);
  assert(file.good());

  if (metadata[0] != 0x11223344) {
    std::cout << "Magic number wrong! Expected " << 0x11223344 << " but got "
              << metadata[0] << std::endl;
    exit(1);
  }

  // If there are no elements
  int elems = metadata[1];
  if (elems == 0) {
    return std::nullopt;
  }

  // If the key is out of bounds for the page
  K lower = metadata[2];
  K upper = metadata[3];
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

std::vector<std::pair<K, V>> SstableNaive::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper) const {
  assert(lower <= upper);

  uint64_t buf[kPageSize];
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(buf), kPageSize);
  assert(file.good());

  if (buf[0] != 0x11223344) {
    std::cout << "Magic number wrong! Expected " << 0x11223344 << " but got "
              << buf[0] << std::endl;
    exit(1);
  }

  std::vector<std::pair<K, V>> l;

  // If there are no elements
  int elems = buf[1];
  if (elems == 0) {
    return l;
  }

  // If the keys are out of bounds for the page
  K page_lower = buf[2];
  K page_upper = buf[3];
  if ((upper < page_lower) || (lower > page_upper)) {
    return l;
  }

  int header_size = 4;
  int pair_size = 2;

  std::optional<BinarySearchResult> res =
      binary_search(file, elems, lower, true);
  assert(res.has_value());

  int walk = res.value().key_index;
  while (buf[walk] <= upper && walk < header_size + elems * pair_size) {
    l.emplace_back(buf[walk], buf[walk + 1]);
    walk += 2;
  }

  return l;
}
