#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "buf.hpp"

class BufPool::BufPoolImpl
{
private:
  std::size_t min_size_;
  std::size_t max_size_;
  std::unordered_map<std::string, std::unordered_map<pageno, Buffer>> pages_;

  static uint32_t hash(const std::string& file, const pageno& page)
  {
    (void)file;
    (void)page;
    return 0;
  }

public:
  BufPoolImpl(std::size_t min_size, std::size_t max_size)
  {
    this->min_size_ = min_size;
    this->max_size_ = max_size;
  }

  ~BufPoolImpl() = default;

  [[nodiscard]] bool HasPage(const std::string& file, const pageno& page) const
  {
    return this->pages_.count(file) > 0
        && this->pages_.at(file).count(page) > 0;
  }

  [[nodiscard]] std::optional<Buffer> GetPage(const std::string& file,
                                              const pageno& page) const
  {
    if (!this->HasPage(file, page)) {
      return std::nullopt;
    }

    return std::make_optional(this->pages_.at(file).at(page));
  }

  void PutPage(const std::string& file,
               const pageno& page,
               const Buffer contents)
  {
    if (this->pages_.count(file) > 0) {
      this->pages_.at(file).insert(std::make_pair(page, contents));
    } else {
      this->pages_.insert(std::make_pair(
          file, std::unordered_map({std::make_pair(page, contents)})));
    }
  }
};

BufPool::BufPool(const std::size_t min_size, const std::size_t max_size)
{
  this->impl_ = std::make_unique<BufPoolImpl>(min_size, max_size);
}

BufPool::~BufPool() = default;

bool BufPool::HasPage(const std::string& file, const pageno& page) const
{
  return this->impl_->HasPage(file, page);
}

std::optional<Buffer> BufPool::GetPage(const std::string& file,
                                       const pageno& page) const
{
  return this->impl_->GetPage(file, page);
}

void BufPool::PutPage(const std::string& file,
                      const pageno& page,
                      const Buffer contents)
{
  return this->impl_->PutPage(file, page, contents);
}