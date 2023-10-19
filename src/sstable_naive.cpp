#include <fstream>
#include <functional>
#include <string>

#include "sstable_naive.hpp"

#include "assert.h"
#include "math.h"
#include "memtable.hpp"

void SstableNaive::flush(std::fstream& file, MemTable& memtable)
{
  std::vector<std::pair<K, V>> pairs = memtable.ScanAll();

  file.open("my-file.bin",
            std::fstream::binary | std::fstream::in | std::fstream::out
                | std::fstream::trunc);
  assert(file.good());

  file << 0x11 << 0x22 << 0x33 << 0x44 << pairs.size() << pairs.front().first
       << pairs.back().first;

  for (const auto [key, value] : pairs) {
    file << key << value;
  }
  file.flush();
  file.close();
};

std::optional<V> SstableNaive::get_from_file(std::fstream& file, const K key)
{
  file.open("s", std::ios_base::binary | std::ios_base::out);
  uint64_t buf[PAGE_SIZE];
  file.seekg(0);
  file.read(reinterpret_cast<char*>(buf), PAGE_SIZE);
  if (buf[0] != 0x11223344) {
    std::cout << "Magic number wrong!" << std::endl;
    exit(1);
  }

  // If there are no elements
  int elems = buf[1];
  if (elems == 0) {
    return std::nullopt;
  }

  // If the key is out of bounds for the page
  int lower = buf[2], upper = buf[3];
  if (key < lower || key > upper) {
    return std::nullopt;
  }

  int header_size = 4;
  int pair_size = 2;

  int left = header_size;
  int right = header_size + elems * pair_size;
  while (left <= right) {
    int mid = left + floor((right - left) / 2);
    assert(mid % 16 == 0);
    if (buf[mid] == key) {
      return buf[mid + 1];
    } else if (buf[mid] < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return std::nullopt;
}

;
std::vector<std::pair<K, V>> SstableNaive::scan_in_file(std::fstream& file,
                                                        const K lower,
                                                        const K upper)
{
  std::vector<std::pair<K, V>> l {};
  return l;
}
