#include "evict.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "buf.hpp"

struct ClockMetadata {
  bool dirty;
  DataRef page;
};

class ClockEvictor::ClockEvictorImpl : Evictor {
 private:
  uint32_t head_;
  std::vector<std::optional<ClockMetadata>> clock_;

 public:
  ClockEvictorImpl() { this->head_ = 0; };
  ~ClockEvictorImpl() override = default;

  std::optional<DataRef> Insert(DataRef page_id) override {
    assert(this->head_ < this->clock_.capacity());

    while (this->clock_.at(this->head_).has_value() &&
           this->clock_.at(this->head_).value().dirty) {
      this->clock_.at(this->head_).value().dirty = false;
      this->head_ = (this->head_ + 1) % this->clock_.capacity();
    }

    std::optional<ClockMetadata> old = this->clock_.at(this->head_);
    this->clock_.at(this->head_)
        .emplace(ClockMetadata{
            .dirty = false,
            .page = page_id,
        });

    this->head_ = (this->head_ + 1) % this->clock_.capacity();

    if (old.has_value()) {
      return std::make_optional<DataRef>(old.value().page);
    }
    return std::nullopt;
  }

  void MarkUsed(DataRef page_id) override {
    for (auto& elem : this->clock_) {
      if (elem.has_value() && elem.value().page == page_id) {
        elem.value().dirty = true;
        return;
      }
    }
  };

  void Resize(uint32_t n) override { this->clock_.resize(n); }
};

ClockEvictor::ClockEvictor() {
  this->impl = std::make_unique<ClockEvictorImpl>();
};

ClockEvictor::~ClockEvictor() = default;

void ClockEvictor::Resize(const uint32_t n) { return this->impl->Resize(n); }

void ClockEvictor::MarkUsed(DataRef page) {
  return this->impl->MarkUsed(page);
};

std::optional<DataRef> ClockEvictor::Insert(DataRef page) {
  return this->impl->Insert(page);
}
