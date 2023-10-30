#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>

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
  std::optional<std::shared_ptr<BufferedPage>> next;
};

struct BufPoolTuning
{
  std::size_t bucket_size;
  std::size_t min_directory_size;
  std::size_t max_directory_size;
};

class BufPool
{
private:
  class BufPoolImpl;
  std::unique_ptr<BufPoolImpl> impl_;

public:
  BufPool(BufPoolTuning tuning, std::unique_ptr<Evictor> evictor);
  ~BufPool();

  [[nodiscard]] bool HasPage(PageId& page) const;
  [[nodiscard]] std::optional<BufferedPage> GetPage(PageId& page) const;
  void PutPage(PageId& page, PageType type, Buffer contents);
};
