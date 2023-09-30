#include <iostream>
#include <string>

#include "kvstore.hpp"

auto main() -> int
{
  auto table = std::make_unique<KvStore>();
  return 0;
}
