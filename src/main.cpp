#include <iostream>
#include <string>

#include "memtable.hpp"

auto main() -> int
{
  auto table = new MemTable<int, std::string>(100);
  int nodes_to_insert = 10;
  for (int i = 1; i < nodes_to_insert + 1; i++) {
    table->Put(i, "val" + std::to_string(i));
  }

  std::cout << table->Print() << std::endl;

  return 0;
}
