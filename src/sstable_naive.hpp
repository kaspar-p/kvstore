#pragma once

#include <fstream>
#include <functional>
#include <string>

#include "memtable.hpp"
#include "sstable.hpp"

typedef uint64_t K;
typedef uint64_t V;

class SstableNaive
{
public:
  SstableNaive() = default;
  void flush(std::fstream& filename, MemTable& memtable);
  std::optional<V> get_from_file(std::fstream& file, const K key);
  std::vector<std::pair<K, V>> scan_in_file(std::fstream& file,
                                            const K lower,
                                            const K upper);
};
