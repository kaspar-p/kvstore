#include <iostream>
#include <string>

#include "memtable.hpp"

auto main() -> int
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Put(2, "wow2!");
  table->Put(3, "wow3!");
  std::cout << table->Print() << std::endl;
  return 0;
}
