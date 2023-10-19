#include <array>
#include <cstdlib>
#include <vector>

#include "stdint.h"

typedef uint64_t K;
typedef uint64_t V;
typedef uint64_t pageno;

#define PAGE_SIZE (4096)

class BufPool
{
private:
  std::size_t initial_size;
  std::size_t max_size;

public:
  BufPool(std::size_t initial_size, std::size_t max_size);

  std::array<uint8_t, PAGE_SIZE> Get(pageno page);
};
