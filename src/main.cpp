#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "kvstore.hpp"
#include "naming.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  MemTable memtable(a);
  for (int i = 0; i < 65280; i++) {
    memtable.Put(i, i);
  }

  SstableBTree t{};
  std::fstream f("/tmp/SstableBTree.GetSingleElems3layers.bin",
                 std::fstream::binary | std::fstream::in | std::fstream::out |
                     std::fstream::trunc);
  assert(f.is_open());
  assert(f.good());
  t.Flush(f, std::make_unique<std::vector<std::pair<K, V>>>(memtable.ScanAll()));

  // FAILING
  // std::optional<V> val = t.GetFromFile(f, 6524); for size = 255*255
  // assert(val.has_value() == true);
  // assert(val.value() == 65024);

  // SCAN
  
  // int m = 254;
  // while (m < 255) {
  //   m++;
  //   std::cout << m << std::endl;
  //   std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 255*m - 1);
  //   std::cout << val.size() << std::endl;
  //   assert(val.size() == 255*m);
  //   for (int i = 0; i < 255*m-1; i++) {
  //     assert(val[i].first == i);
  //     assert(val[i].second == i);
  //   }
  // }

  // std::vector<std::pair<K, V>> val = t.ScanInFile(f, 0, 65024);
  // std::cout << val.size() << std::endl;
  // assert(val.size() == 65025);
  // for (int i = 0; i < 65024; i++) {
  //   assert(val[i].first == i);
  //   assert(val[i].second == i);
  // }


  // SINGLE GET
  // std::optional<V> val = t.GetFromFile(f, 65025);
  // assert(val.has_value());
  // assert(val.value() == 65025);

  // RANGE GET
  // when m = 510 (2 leaves), 254 is missing
  // when m = 765 (3 leaves), 254 to 509 are missing
  // when m = 1020 (4 leaves), 254 to 764 are missing
  int total = 0;
  for (int i = 255*254; i < 65025; i++) {
    std::optional<V> val = t.GetFromFile(f, i);
    std::cout << i << std::endl;
    total += 1;
    assert(val.has_value());
    assert(val.value() == i);
  }
  std::cout << total << std::endl;
}
