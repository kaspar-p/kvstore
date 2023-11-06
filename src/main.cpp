#include <cassert>
#include <exception>
#include <filesystem>
#include <iostream>

#include "dbg.hpp"
#include "filter.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "xxhash.h"

int main() {
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 4,
  });

  PageId id = {.filename = std::string("unique_file"), .page = 2};
  for (int i = 0; i < 10; i++) {
    buf.PutPage(id, PageType::kFilters, Buffer{std::byte{(uint8_t)i}});
  }

  std::optional<BufferedPage> page = buf.GetPage(id);
  assert(page.has_value());
  assert(page.value().type == PageType::kFilters);
  assert(page.value().contents == Buffer{std::byte{9}});
}
