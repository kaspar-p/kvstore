#include <fstream>
#include <functional>
#include <string>

#include "sstable.hpp"

#include "assert.h"
#include "math.h"
#include "memtable.hpp"

void SstableBTree::Flush(std::fstream& file, MemTable& memtable)
{
  return;
};

std::optional<V> SstableBTree::GetFromFile(std::fstream& file, const K key)
{
  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableBTree::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper)
{
  std::vector<std::pair<K, V>> l {};
  return l;
};
