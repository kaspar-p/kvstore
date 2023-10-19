#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

#include "kvstore.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "sstable_naive.hpp"

int main()
{
  std::cout << "STARTING" << std::endl;

  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t {};
  std::fstream f;
  t.flush(f, memtable);
  f.close();

  std::cout << "ENDING" << std::endl;

  return 0;
}
