#include <exception>
#include <iostream>

#include "dbg.hpp"
#include "filter.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  std::cout << "STARTING" << '\n';
  uint64_t k1 = 128;
  uint64_t h1 = XXH64(&k1, sizeof(k1), 100);
  std::cout << "h1: " << h1 << ' ' << bit_string(h1, 64) << '\n';

  uint64_t k2 = 129;
  uint64_t h2 = XXH64(&k2, sizeof(k2), 100);
  std::cout << "h2: " << h2 << ' ' << bit_string(h2, 64) << '\n';

  BufPool buf(BufPoolTuning{.initial_elements = 16, .max_elements = 16},
              std::make_unique<ClockEvictor>(), &Hash);
  Filter f("name", 1, 64, buf, 100);
  f.Put(100);
  bool val = f.Has(100);
  std::cout << val << "\n";
}
