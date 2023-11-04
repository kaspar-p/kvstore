#include <fstream>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

void SstableBTree::Flush(std::fstream& file, MemTable& memtable) {
  (void)file;
  (void)memtable;
};

std::optional<V> SstableBTree::GetFromFile(std::fstream& file, const K key) {
  (void)file;
  (void)key;
  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableBTree::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper) {
  (void)file;
  (void)lower;
  (void)upper;
  std::vector<std::pair<K, V>> l{};
  return l;
};
