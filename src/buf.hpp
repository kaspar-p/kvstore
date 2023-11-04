#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "constants.hpp"
#include "evict.hpp"

using pageno = uint64_t;

enum PageType
{
  kBTreeLeaf = 0,
  kBTreeInternal = 1,
  kMetadata = 2,
};

using Buffer = std::array<std::byte, kPageSize>;

struct PageId
{
  uint32_t level;
  uint32_t run;
  uint32_t page;

  [[nodiscard]] std::string str() const;
  [[nodiscard]] std::string str(uint32_t len) const;

  bool operator==(const PageId& other) const
  {
    return this->level == other.level && this->run == other.run
        && this->page == other.page;
  }

  bool operator!=(const PageId& other) const { return !(*this == other); }
};

struct BufferedPage
{
  PageId id;
  PageType type;
  Buffer contents;
  bool operator==(BufferedPage& other) const { return this->id == other.id; }
};

struct BufPoolTuning
{
  uint32_t initial_elements;
  uint32_t max_elements;
};

class BufPool
{
private:
  class BufPoolImpl;
  std::unique_ptr<BufPoolImpl> impl_;

public:
  BufPool(BufPoolTuning tuning,
          std::unique_ptr<Evictor> evictor,
          uint32_t (*hash)(const PageId&));
  ~BufPool();

  [[nodiscard]] bool HasPage(PageId& page) const;
  [[nodiscard]] std::optional<BufferedPage> GetPage(PageId& page) const;
  void PutPage(PageId& page, PageType type, Buffer contents);

  std::string DebugPrint(uint32_t);
  std::string DebugPrint();
};
