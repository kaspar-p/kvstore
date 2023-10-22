#include <exception>
#include <fstream>
#include <iostream>

#include "memtable.hpp"
#include "sstable.hpp"

int main()
{
  std::cout << "STARTING" << '\n';
  try {
    MemTable memtable(64);
    for (int i = 0; i < 64; i++) {
      memtable.Put(i, i);
    }
    std::fstream f;
    SstableNaive::Flush(f, memtable);

    auto val = SstableNaive::GetFromFile(f, 33).value();

    std::cout << "ENDING " << val << '\n';

    return 0;
  } catch (std::exception& e) {
    std::cerr << e.what() << '\n';
  }
}
