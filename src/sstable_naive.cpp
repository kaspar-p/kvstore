#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

void SstableNaive::Flush(std::fstream& file, MemTable& memtable) {
  assert(file.is_open());
  assert(file.good());
  std::vector<std::pair<K, V>> pairs = memtable.ScanAll();

  file.seekp(0);
  assert(file.good());

  const size_t actual_size = 4 + 2 * pairs.size();
  const size_t bufsize = kPageSize;
  uint64_t wbuf[bufsize];
  wbuf[0] = 0x11223344;
  wbuf[1] = pairs.size();
  wbuf[2] = pairs.front().first;
  wbuf[3] = pairs.back().first;
  for (int i = 0; i < pairs.size(); i++) {
    wbuf[4 + (2 * i) + 0] = pairs[i].first;
    wbuf[4 + (2 * i) + 1] = pairs[i].second;
  }
  // Pad the rest of the page with zeroes
  for (int i = actual_size; i < bufsize; i++) {
    wbuf[i] = 0;
  }

  for (uint64_t& elem : wbuf) {
    file.write(reinterpret_cast<char*>(&elem), sizeof(uint64_t));
    assert(file.good());
  }

  file.flush();
  assert(file.good());
};

std::optional<V> SstableNaive::GetFromFile(std::fstream& file, const K key) {
  uint64_t buf[kPageSize];
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);

  file.read(reinterpret_cast<char*>(buf), kPageSize);
  assert(file.good());

  if (buf[0] != 0x11223344) {
    std::cout << "Magic number wrong! Expected " << 0x11223344 << " but got "
              << buf[0] << '\n';
    exit(1);
  }

  // If there are no elements
  int elems = buf[1];
  if (elems == 0) {
    return std::nullopt;
  }

  // If the key is out of bounds for the page
  K lower = buf[2];
  K upper = buf[3];
  if (key < lower || key > upper) {
    return std::nullopt;
  }

  int header_size = 4;
  int pair_size = 2;

  int left = header_size;
  int right = header_size + elems * pair_size;
  while (left <= right) {
    int mid = left + floor((right - left) / 2);
    if (buf[mid] == key) {
      return std::make_optional(buf[mid]);
    }

    if (buf[mid] < key) {
      left = mid + 2;
    } else {
      right = mid - 2;
    }
  }

  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableNaive::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper) {
  uint64_t buf[kPageSize];
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(buf), kPageSize);
  assert(file.good());

  if (buf[0] != 0x11223344) {
    std::cout << "Magic number wrong! Expected " << 0x11223344 << " but got "
              << buf[0] << '\n';
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
  if ((lower < page_lower && upper < page_lower) ||
      (lower > page_upper && upper > page_upper)) {
    return l;
  }

  int header_size = 4;
  int pair_size = 2;

  int left = header_size;
  int right = header_size + elems * pair_size;
  int mid = left + floor((right - left) / 4) * 2;
  while (left <= right) {
    mid = left + floor((right - left) / 4) * 2;
    assert(mid % 2 == 0);
    if (left == right || buf[mid] == lower) {
      break;
    }

    if (buf[mid] < lower) {
      left = mid + pair_size;
    } else {
      right = mid - pair_size;
    }
  }
  assert(buf[mid] >= lower);

  int walk = mid;
  while (buf[walk] <= upper && walk < header_size + elems * pair_size) {
    l.emplace_back(buf[walk], buf[walk + 1]);
    walk += 2;
  }

  return l;
}
