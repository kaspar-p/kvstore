#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "evict.hpp"

#include "buf.hpp"

struct ClockMetadata
{
  bool dirty;
  ChainedPage* page;
};

class ClockEvictor::ClockEvictorImpl : Evictor
{
private:
  uint32_t head_;
  std::vector<std::optional<ClockMetadata>> clock_;
  std::unordered_map<ChainedPage*, uint32_t> indices_;

public:
  ClockEvictorImpl() { this->head_ = 0; };

  ~ClockEvictorImpl() = default;

  std::optional<ChainedPage*> Insert(ChainedPage* page) override
  {
    assert(this->head_ < this->clock_.capacity());

    while (this->clock_.at(this->head_).has_value()
           && this->clock_.at(this->head_).value().dirty)
    {
      this->clock_.at(this->head_).value().dirty = false;
      this->head_ = (this->head_ + 1) % this->clock_.capacity();
    }

    std::optional<ClockMetadata> old = this->clock_.at(this->head_);
    this->clock_.at(this->head_) = std::make_optional(ClockMetadata {
        .dirty = false,
        .page = page,
    });
    this->indices_.insert(std::make_pair(page, this->head_));
    this->head_ = (this->head_ + 1) % this->clock_.capacity();

    if (old.has_value()) {
      this->indices_.erase(old.value().page);
      return std::make_optional(old.value().page);
    }
    return std::nullopt;
  }

  void MarkUsed(ChainedPage* page) override
  {
    if (this->indices_.find(page) != this->indices_.end()) {
      uint32_t index = this->indices_.at(page);
      assert(index < this->clock_.capacity());

      this->clock_.at(index).value().dirty = true;
    }
  };

  void Resize(uint32_t n) override { this->clock_.resize(n); }
};

ClockEvictor::ClockEvictor()
{
  this->impl_ = std::make_unique<ClockEvictorImpl>();
};

ClockEvictor::~ClockEvictor() = default;

void ClockEvictor::Resize(const uint32_t n)
{
  return this->impl_->Resize(n);
}

void ClockEvictor::MarkUsed(ChainedPage* page)
{
  return this->impl_->MarkUsed(page);
};

std::optional<ChainedPage*> ClockEvictor::Insert(ChainedPage* page)
{
  return this->impl_->Insert(page);
}
