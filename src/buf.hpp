#pragma once

#include <any>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "constants.hpp"
#include "evict.hpp"
#include "xxhash.h"

using pageno = uint64_t;

enum PageType {
  kBTreeLeaf = 0,
  kBTreeInternal = 1,
  kMetadata = 2,
  kFilters = 3
};

using BytePage = std::array<std::byte, kPageSize>;

struct PageId {
  std::string filename;
  uint32_t page;

  [[nodiscard]] std::string str() const;
  [[nodiscard]] std::string str(uint32_t len) const;

  bool operator==(const PageId& other) const {
    return this->filename == other.filename && this->page == other.page;
  }

  bool operator!=(const PageId& other) const { return !(*this == other); }
};

struct BufferedPage {
  PageId id;
  std::any contents;
  bool operator==(BufferedPage& other) const { return this->id == other.id; }
};

struct BufPoolTuning {
  std::size_t initial_elements;
  std::size_t max_elements;
};

using PageHashFn = std::function<uint32_t(const PageId&)>;
uint32_t Hash(const PageId& page_id);

class BufPool {
 private:
  class BufPoolImpl;
  const std::unique_ptr<BufPoolImpl> impl;

 public:
  BufPool(BufPoolTuning tuning, std::unique_ptr<Evictor> evictor,
          PageHashFn hash);
  BufPool(BufPoolTuning tuning, PageHashFn Hash);
  BufPool(BufPoolTuning tuning);
  ~BufPool();

  [[nodiscard]] bool HasPage(PageId& page) const;

  [[nodiscard]] std::optional<BufferedPage> GetPage(PageId& page) const;
  void PutPage(PageId& page, std::any contents);

  std::string DebugPrint(uint32_t);
  std::string DebugPrint();
};
