#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "filter.hpp"
#include "kvstore.hpp"
#include "memtable.hpp"
#include "naming.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  // std::filesystem::remove_all("kvstore.db");

  // Options opt = {
  // .memory_buffer_elements = 2,
  // .tiers = 4,
  // .serialization = DataFileFormat::kBTree,
  // };
  // KvStore kv;
  // kv.Open("kvstore.db", opt);

  // int runs_in_level0_to_make = 80;

  // for (int i = 0; i < 2 * runs_in_level0_to_make; i++) {
  // std::cout << "putting " << i << std::endl;
  // kv.Put(i, 2 * i);
  // }
  DbNaming naming = {.dirpath = "/tmp",
                     .name = "KvStore.InsertAndDeleteThousands"};
  BufPool buf(BufPoolTuning{.initial_elements = 2, .max_elements = 16});
  SstableBTree serializer(buf);
  Filter filter(naming, buf, 0);

  std::string f =
      "/tmp/KvStore.InsertAndDeleteThousands/"
      "KvStore.InsertAndDeleteThousands.FILTER.L0.R0.I0";
  std::cout << "HAS" << filter.Has(f, 600) << '\n';

  std::string prefix =
      "/tmp/KvStore.InsertAndDeleteThousands/"
      "KvStore.InsertAndDeleteThousands.DATA.";

  // std::string d = prefix + "L0.R0.I0";
  // std::cout << d << "\n";
  // auto v = serializer.Drain(d);
  // for (auto &elem : v) {
  //   std::cout << elem.first << "," << elem.second << '\n';
  // }

  // d = prefix + "L1.R0.I0";
  // std::cout << d << "\n";
  // v = serializer.Drain(d);
  // for (auto &elem : v) {
  //   std::cout << elem.first << "," << elem.second << '\n';
  // }

  // d = prefix + "L1.R0.I1";
  // std::cout << d << "\n";
  // v = serializer.Drain(d);
  // for (auto &elem : v) {
  //   std::cout << elem.first << "," << elem.second << '\n';
  // }

  // SstableBTree t{};
  // std::fstream f("/tmp/SstableBTree.GetSingleElems3layers",
  //                std::fstream::binary | std::fstream::in | std::fstream::out
  //                |
  //                    std::fstream::trunc);
  // assert(f.is_open());
  // assert(f.good());
  // auto v = memtable.ScanAll();
  // t.Flush(f, *v);

  // for (int i = 0; i < 5; i++) {
  //   std::cout << "putting " << i << std::endl;
  //   kv.Put(i, 2 * i);
  // }

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
  // int total = 0;
  // for (int i = 255*254; i < 65025; i++) {
  //   std::optional<V> val = t.GetFromFile(f, i);
  //   std::cout << i << std::endl;
  //   total += 1;
  //   assert(val.has_value());
  //   assert(val.value() == i);
  // }
  // std::cout << total << std::endl;

  // KASPAR TEST
  // MemTable memtable(2000);
  // for (int i = 0; i < 2000; i++) {
  //   memtable.Put(i, 2 * i);
  // }

  // SstableBTree t{};
  // std::fstream f("/tmp/SstableBTree.GetManySingleElems",
  //                std::fstream::binary | std::fstream::in | std::fstream::out
  //                |
  //                    std::fstream::trunc);
  // assert(f.is_open());
  // assert(f.good());
  // auto pairs = memtable.ScanAll();
  // t.Flush(f, *pairs);

  // for (int i = 0; i < 2000; i++) {
  //   std::optional<V> val = t.GetFromFile(f, i);
  //   assert(val.has_value());
  //   assert(val.value() == 2 * i);
  // }
}