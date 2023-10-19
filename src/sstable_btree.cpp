#include <fstream>
#include <functional>
#include <string>

#include "sstable.hpp"

#include "assert.h"
#include "math.h"
#include "memtable.hpp"

void SstableBTree::flush(std::fstream& file, MemTable& memtable) {
    //
};

std::optional<V> SstableBTree::get_from_file(std::fstream& file, const K key)
{
  return std::nullopt;
};
std::vector<std::pair<K, V>> SstableBTree::scan_in_file(std::fstream& file,
                                                        const K lower,
                                                        const K upper)
{
  std::vector<std::pair<K, V>> l {};
  return l;
};
