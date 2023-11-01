#include <bitset>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
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
  const uint32_t initial_elements_;
  const uint32_t max_elements_;
  const std::unique_ptr<Evictor> evictor_;

  uint16_t bits_;
  uint32_t capacity_;
  uint32_t elements_;
  std::vector<ChainedPage*> directory_;

  void place_front(uint32_t relevant_bits, ChainedPage* page)
  {
    ChainedPage* curr = this->directory_.at(relevant_bits);
    page->next = curr;
    if (curr != nullptr) {
      curr->prev = page;
    }
    this->directory_.at(relevant_bits) = page;
  }

  void consider_resizing(uint32_t relevant_bits)
  {
    const double thresh = 0.8F;

    bool needs_to_resize = this->elements_ >= floor(this->capacity_ * thresh);
    bool can_resize = this->capacity_ < this->max_elements_;
    if (!needs_to_resize || !can_resize) {
      return;  // no need to resize
    }

    this->capacity_ *= 2;
    this->bits_ += 1;

    // Split the pointers
    std::vector<ChainedPage*> new_directory;
    new_directory.resize(pow(2, this->bits_));
    for (int i = 0; i < pow(2, this->bits_ - 1); i++) {
      new_directory.at(2 * i + 0) = this->directory_.at(i);
      new_directory.at(2 * i + 1) = this->directory_.at(i);
    }

    this->directory_ = new_directory;

    // Split the nodes with large local depth
    // Check for local depth
    // for (ChainedPage* chain : this->directory_) {
    //   while (chain != nullptr) {
    //     this->place_front(this->relevant_bits(this->bits_, chain->page.id),
    //                       chain);
    //     chain = chain->next;
    //   }
    // }
  }

  void consider_evicting(uint32_t relevant_bits, ChainedPage* entry)
  {
    std::optional<ChainedPage*> evicted = this->evictor_->Insert(entry);
    if (!evicted.has_value()) {
      return;  // no need to evict
    }

    auto* prev = evicted.value()->prev;
    auto* next = evicted.value()->next;

    bool is_alone = prev == nullptr && next == nullptr;
    bool is_first = prev == nullptr && next != nullptr;
    bool is_last = prev != nullptr && next == nullptr;
    bool is_middle = prev != nullptr && next != nullptr;

    if (is_alone) {
      this->directory_.at(relevant_bits) = nullptr;
    } else if (is_first) {
      this->directory_.at(relevant_bits) = next;
      next->prev = nullptr;
    } else if (is_last) {
      prev->next = nullptr;
    } else if (is_middle) {
      next->prev = prev;
      prev->next = next;
    }

    delete evicted.value();
  }

  void place_entry(uint32_t relevant_bits,
                   ChainedPage** addr,
                   ChainedPage* entry)
  {
    *addr = entry;
    this->elements_ += 1;
    this->consider_resizing(relevant_bits);
    this->consider_evicting(relevant_bits, entry);
  }

  std::string bit_string(uint32_t bits, uint32_t num)
  {
    std::ostringstream s;

    for (int j = floor(log2(fmax(num, 1))); j >= 0; j--) {
      s << std::to_string((num >> j) & 1);
    }

    std::ostringstream s2;
    for (int i = bits - s.str().size() - 1; i >= 0; i--) {
      s2 << '0';
    }
    s2 << s.str();

    return s2.str();
  }

  [[nodiscard]] uint32_t relevant_bits(uint32_t bits_to_use,
                                       const PageId& id) const
  {
    return hash(id) & (UINT32_MAX >> (32 - bits_to_use));
  }

public:
  BufPoolImpl(uint32_t initial_elements,
              uint32_t max_elements,
              std::unique_ptr<Evictor> evictor)
      : initial_elements_(initial_elements)
      , max_elements_(max_elements)
      , evictor_(std::move(evictor))
  {
    this->elements_ = 0;
    this->bits_ = fmax(ceil(log2l(initial_elements)), 1);
    this->capacity_ = fmax(pow(2, ceil(log2l(initial_elements))), 2);
    this->evictor_->Resize(max_elements);
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
    uint32_t bits = this->relevant_bits(this->bits_, page_id);
    ChainedPage* chain = this->directory_.at(bits);

    while (chain != nullptr && chain->page.id != page_id) {
      chain = chain->next;
    }

    if (chain != nullptr) {
      assert(chain->page.id == page_id);
      this->evictor_->MarkUsed(chain);
      return chain->page;
    }

    return std::nullopt;
  }

  void PutPage(const PageId& page_id,
               const PageType type,
               const Buffer contents)
  {
    uint32_t relevant_bits = this->relevant_bits(this->bits_, page_id);
    ChainedPage* chain = this->directory_.at(relevant_bits);

    auto* ent = new ChainedPage();
    ent->next = nullptr;
    ent->prev = nullptr;
    ent->page = BufferedPage {
        .id = page_id,
        .type = type,
        .contents = contents,
    };

    // If we found an empty chain
    if (chain == nullptr) {
      return this->place_entry(
          relevant_bits, &this->directory_[relevant_bits], ent);
    }

    ChainedPage* prev = chain;
    ChainedPage* curr = prev->next;
    while (curr != nullptr) {
      prev = curr;
      curr = curr->next;
    }
    assert(prev->next == nullptr);
    this->place_entry(relevant_bits, &prev->next, ent);
  }

  [[nodiscard]] std::string DebugPrint()
  {
    std::ostringstream s;
    s << "max: " << std::to_string(this->max_elements_) << "\n";
    s << "elements: " << std::to_string(this->elements_) << "\n";
    s << "bits: " << std::to_string(this->bits_) << "\n";
    s << "capacity: " << std::to_string(this->capacity_) << "\n";
    for (int i = 0; i < this->directory_.size(); i++) {
      auto* ent = this->directory_.at(i);
      s << "[" << this->bit_string(this->bits_, i) << "]: ";

      while (ent != nullptr) {
        s << "(" << std::to_string(ent->page.id.level) << ","
          << std::to_string(ent->page.id.run) << ","
          << std::to_string(ent->page.id.page) << ")"
          << " -> ";
        ent = ent->next;
      }
      s << "()\n";
    }

    return s.str();
  }
};

BufPool::BufPool(const BufPoolTuning tuning, std::unique_ptr<Evictor> evictor)
{
  this->impl_ = std::make_unique<BufPoolImpl>(
      tuning.initial_elements, tuning.max_elements, std::move(evictor));
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

std::string BufPool::DebugPrint()
{
  return this->impl_->DebugPrint();
}
