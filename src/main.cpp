#include <iostream>
#include <string>

#include "kvstore.hpp"

int main()
{
  MemTable* table = new MemTable(2);

  // std::cout << table->Print() << std::endl;

  delete table;

  return 0;
}
