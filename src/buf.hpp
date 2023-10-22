#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>

#include "constants.hpp"

using pageno = uint64_t;

enum PageType
{
  kBTreeLeaf = 0,
  kBTreeInternal = 1,
  kMetadata = 2,
};

using Buffer = std::array<std::byte, kPageSize>;

class BufPool
{
private:
  class BufPoolImpl;
  std::unique_ptr<BufPoolImpl> impl_;

public:
  BufPool(std::size_t min_size, std::size_t max_size);
  ~BufPool();

  [[nodiscard]] bool HasPage(const std::string& file, const pageno& page) const;
  [[nodiscard]] std::optional<Buffer> GetPage(const std::string& file,
                                              const pageno& page) const;
  void PutPage(const std::string& file, const pageno& page, Buffer contents);
};
