#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

#include "kvstore.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

int main()
{
  std::cout << "STARTING" << std::endl;

  MemTable memtable(64);
  for (int i = 0; i < 64; i++) {
    memtable.Put(i, i);
  }
  SstableNaive t {};
  std::fstream f;
  t.Flush(f, memtable);

  auto val = t.GetFromFile(f, 33).value();

  std::cout << "ENDING " << val << std::endl;

  return 0;
}
