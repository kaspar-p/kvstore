#include <iostream>
#include <string>

#include "memtable.hpp"

auto main() -> int
{
  auto table = new MemTable<int, std::string>(100);
  table->Put(1, "wow1!");
  table->Delete(1);

  std::cout << table->Print() << std::endl;

  return 0;
}
