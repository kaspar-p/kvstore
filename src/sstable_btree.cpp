#include <functional>
#include <iostream>
#include <string>

#include "sstable.hpp"

#include "memtable.hpp"

class SstableBtree : public Sstable
{
public:
  void flush(std::unique_ptr<std::fstream> file, MemTable memtable)
  {
    //
  }

  std::optional<V> get_from_file(std::unique_ptr<std::fstream> file,
                                 const K key)
  {
    return std::nullopt;
  }

  std::vector<std::pair<K, V>> scan_in_file(std::unique_ptr<std::fstream> file,
                                            const K lower,
                                            const K upper)
  {
    std::vector<std::pair<K, V>> l {};
    return l;
  }
};
