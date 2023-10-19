#include "buf.hpp"

BufPool::BufPool(std::size_t initial_size, std::size_t max_size)
{
  this->initial_size = initial_size;
  this->max_size = max_size;
}

std::array<uint8_t, PAGE_SIZE> BufPool::Get(pageno page)
{
  return std::array<uint8_t, PAGE_SIZE>();
}
