#include <cassert>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "buf.hpp"

#include "evict.hpp"

uint32_t hash(const PageId& page)
{
  return page.level + page.run + page.page;
}

class BufPool::BufPoolImpl
{
private:
  const uint8_t bucket_size_;
  const uint8_t min_size_;
  const uint8_t max_size_;
  const std::unique_ptr<Evictor> evictor_;

  uint8_t bits_;
  std::vector<std::optional<std::shared_ptr<BufferedPage>>> directory_;

  /**
   * @brief Resizes the current directory, either adding a bit (doubling
   * capacity), or removing a bit (halving capacity), based on the bounds.
   *
   * Assumes that resizing is possible and necessary.
   */
  void resize(uint32_t) { assert(this->bits_ < this->max_size_); }

  /**
   * @brief When a bucket is full, either resize to more buckets,
   * or evict a page to make space, if we are already full.
   *
   * @param bits The index into `this->directory_` of the full bucket.
   * Note that there may be other buckets that are full, but this one
   * is guaranteed to be full.
   */
  void resize_or_evict(uint32_t bits)
  {
    if (this->bits_ < this->max_size_) {
      this->resize(bits);
    }
  }

public:
  BufPoolImpl(uint8_t bucket_size,
              uint32_t min_directory_bits,
              uint32_t max_directory_bits,
              std::unique_ptr<Evictor> evictor)
      : bucket_size_(bucket_size)
      , min_size_(min_directory_bits)
      , max_size_(max_directory_bits)
      , evictor_(std::move(evictor))
  {
    this->bits_ = min_directory_bits;
    this->evictor_->Resize(pow(2, this->bits_) * this->bucket_size_);
    this->directory_.resize(pow(2, this->bits_));
  }

  ~BufPoolImpl() = default;

  [[nodiscard]] bool HasPage(const PageId& page) const
  {
    const std::optional<BufferedPage> p = this->GetPage(page);
    return p.has_value();
  }

  [[nodiscard]] std::optional<BufferedPage> GetPage(const PageId& page_id) const
  {
    const uint32_t h = hash(page_id);
    uint32_t bits = h >> (32 - this->bits_);
    std::optional<std::shared_ptr<BufferedPage>> page =
        this->directory_.at(bits);
    while (page.has_value() && page.value()->id != page_id) {
      page = page.value()->next;
    }

    if (page.has_value()) {
      assert(page.value()->id == page_id);
      return *page.value();
    }

    return std::nullopt;
  }

  void PutPage(const PageId& page_id,
               const PageType type,
               const Buffer contents)
  {
    const uint32_t h = hash(page_id);
    uint32_t bits = h >> (32 - this->bits_);
    std::optional<std::shared_ptr<BufferedPage>> page =
        this->directory_.at(bits);

    std::optional<std::shared_ptr<BufferedPage>> ent =
        std::make_optional(std::make_shared<BufferedPage>(BufferedPage {
            .id = page_id,
            .type = type,
            .contents = contents,
            .next = std::nullopt,
        }));

    if (!page.has_value()) {
      this->directory_[bits] = ent;
      return;
    }

    std::shared_ptr<BufferedPage> prev = page.value();
    std::optional<std::shared_ptr<BufferedPage>> curr = prev->next;
    for (int i = 0; i < this->bucket_size_; i++) {
      if (curr.has_value()) {
        prev = curr.value();
        curr = curr.value()->next;
      } else {
        prev->next = ent;
        break;
      }
    }

    bool failed_to_place = curr.has_value();
    if (failed_to_place) {
      bool can_resize = this->bits_ < this->max_size_;
      if (can_resize) {
        this->resize(bits);
      }
    }
  }
};

BufPool::BufPool(const BufPoolTuning tuning, std::unique_ptr<Evictor> evictor)
{
  this->impl_ = std::make_unique<BufPoolImpl>(tuning.bucket_size,
                                              tuning.min_directory_size,
                                              tuning.max_directory_size,
                                              std::move(evictor));
}

BufPool::~BufPool() = default;

bool BufPool::HasPage(PageId& page) const
{
  return this->impl_->HasPage(page);
}

std::optional<BufferedPage> BufPool::GetPage(PageId& page) const
{
  return this->impl_->GetPage(page);
}

void BufPool::PutPage(PageId& page, const PageType type, const Buffer contents)
{
  return this->impl_->PutPage(page, type, contents);
}