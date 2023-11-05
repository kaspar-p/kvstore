#include "buf.hpp"

#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "dbg.hpp"
#include "evict.hpp"
#include "xxhash.h"

uint32_t Hash(const PageId& page_id) {
  return XXH32(&page_id.page, sizeof(uint32_t), 99);
};

Buffer FromRaw(char buf[kPageSize]) {
  Buffer buffer;
  for (int i = 0; i < kPageSize; i++) {
    buffer[i] = std::byte{(uint8_t)buf[i]};
  }
  return buffer;
}

char* ToRaw(Buffer& in, char out[kPageSize]) {
  for (int i = 0; i < kPageSize; i++) {
    out[i] = (uint8_t)in[i];
  }
  return out;
}

[[nodiscard]] std::string PageId::str() const { return this->str(32); }
[[nodiscard]] std::string PageId::str(uint32_t len) const {
  std::ostringstream s;
  s << "(" << this->filename << "," << bit_string(this->page, len) << ")";
  return s.str();
}

struct TrieNode {
  // uint32_t hash;
  uint32_t prefix_length;
  // If has_value(), then children array is nullopt
  std::optional<std::list<BufferedPage>> bucket;
  // If has_value(), then bucket is nullopt
  std::optional<std::array<std::unique_ptr<TrieNode>, 2>> children;
};

std::string bucket_print(std::list<BufferedPage>& bucket, uint32_t bit_length) {
  std::ostringstream s;
  for (auto& elem : bucket) {
    s << elem.id.str(bit_length) << " -> ";
  }
  s << "()";
  return s.str();
}

std::string trie_print(TrieNode& node, std::string&& prefix, uint32_t offset,
                       uint32_t bit_length) {
  std::string delim(".");
  std::ostringstream s;

  s << repeat(delim, offset * 2) << "[" << prefix << ']';
  if (node.children.has_value()) {
    s << ":\n";
    s << trie_print(*node.children.value().at(0), prefix + "0", offset + 1,
                    bit_length);
    s << trie_print(*node.children.value().at(1), prefix + "1", offset + 1,
                    bit_length);
  } else {
    s << ": " << bucket_print(node.bucket.value(), bit_length) << '\n';
  }

  return s.str();
}

class BufPool::BufPoolImpl {
 private:
  const uint32_t initial_elements_;
  const uint32_t max_elements_;
  uint32_t (*hash_func_)(const PageId&);
  const std::unique_ptr<Evictor> evictor;

  uint16_t bits_;
  uint32_t capacity_;
  uint32_t elements_;
  std::unique_ptr<TrieNode> root_;

  /**
   * @brief Returns true if replaced a node, false if added new node
   */
  static bool trie_put_in_bucket(TrieNode& node, BufferedPage page) {
    assert(node.bucket.has_value());
    assert(!node.children.has_value());
    bool removed = false;
    node.bucket.value().remove_if([&page, &removed](const BufferedPage& entry) {
      if (entry.id == page.id) {
        removed = true;
        return true;
      } else {
        return false;
      }
    });
    node.bucket.value().push_front(page);
    return removed > 0;
  }

  [[nodiscard]] TrieNode& trie_find_bucket(uint32_t hash,
                                           uint32_t prefix_length) const {
    int offset = 0;
    uint32_t idx;

    std::reference_wrapper<TrieNode> curr = *this->root_;
    while (curr.get().children.has_value()) {
      assert(offset < prefix_length);
      idx = prefix_bit(hash, offset);
      curr = *curr.get().children.value().at(idx);
      offset += 1;
    }
    assert(curr.get().bucket.has_value());
    return curr.get();
  }

  void trie_erase(const PageId& page_id) {
    TrieNode& bucket = this->trie_find_bucket(hash_func_(page_id), this->bits_);
    BufPoolImpl::trie_erase_from_bucket(bucket, page_id);
    this->elements_ -= 1;
  }

  static void trie_erase_from_bucket(TrieNode& node, const PageId& page_id) {
    assert(node.bucket.has_value());
    assert(!node.children.has_value());

    // I think this is O(n) :(
    node.bucket.value().remove_if(
        [page_id](BufferedPage& elem) { return elem.id == page_id; });
  }

  /**
   * @brief Splits a Trie node that represents something like 0* into 00* and
   * 01*. We assume that the Trie node is final, e.g. that it is a bucket, and
   * has no children. We split the chain contents of the node into the new two
   * children that will be created from this split. This function assumes a
   * split is necessary, does not do the check itself.
   *
   * @param node The node to split.
   */
  void trie_split(TrieNode& node) {
    assert(node.bucket.has_value());
    assert(!node.children.has_value());

    std::unique_ptr<TrieNode> left = std::make_unique<TrieNode>();
    left->bucket = std::make_optional<std::list<BufferedPage>>({});
    left->children = std::nullopt;
    left->prefix_length = node.prefix_length + 1;
    std::unique_ptr<TrieNode> right = std::make_unique<TrieNode>();
    right->bucket = std::make_optional<std::list<BufferedPage>>({});
    right->children = std::nullopt;
    right->prefix_length = node.prefix_length + 1;

    for (auto page : node.bucket.value()) {
      int last_bit = prefix_bit(hash_func_(page.id), node.prefix_length);
      if (last_bit == 0) {
        BufPoolImpl::trie_put_in_bucket(*left, page);
      } else {
        BufPoolImpl::trie_put_in_bucket(*right, page);
      }
    }

    node.bucket = std::nullopt;
    node.children.emplace(std::array<std::unique_ptr<TrieNode>, 2>{
        std::move(left), std::move(right)});
  }

  void trie_scan_to_split(TrieNode& root) {
    TrieNode& curr = root;
    if (curr.children.has_value()) {
      BufPoolImpl::trie_scan_to_split(*curr.children.value()[0]);
      BufPoolImpl::trie_scan_to_split(*curr.children.value()[1]);
    } else {
      if (pow(2, this->bits_ - root.prefix_length) > this->bits_) {
        BufPoolImpl::trie_split(root);
      }
    }
  }

  void consider_resizing() {
    const double thresh = 0.8F;

    bool needs_to_resize = this->elements_ >= floor(this->capacity_ * thresh);
    bool can_resize = this->capacity_ < this->max_elements_;
    if (!needs_to_resize || !can_resize) {
      return;  // no need to resize
    }

    this->capacity_ *= 2;
    this->bits_ += 1;

    // Split the pointers
    this->trie_scan_to_split(*this->root_);
  }

  void consider_evicting(BufferedPage& entry) {
    auto evicted = this->evictor->Insert(entry.id);
    if (!evicted.has_value()) {
      return;  // no need to evict
    }

    this->trie_erase(evicted.value());
  }

 public:
  BufPoolImpl(uint32_t initial_elements, uint32_t max_elements,
              std::unique_ptr<Evictor> evictor, uint32_t (*hash)(const PageId&))
      : initial_elements_(initial_elements),
        max_elements_(max_elements),
        hash_func_(hash),
        evictor(std::move(evictor)) {
    this->elements_ = 0;
    this->bits_ = fmax(ceil(log2l(initial_elements)), 1);
    this->capacity_ = fmax(pow(2, ceil(log2l(initial_elements))), 2);
    this->evictor->Resize(max_elements);

    this->root_ = std::make_unique<TrieNode>(TrieNode{
        .prefix_length = 0,
        .bucket = std::make_optional(std::list<BufferedPage>{}),
        .children = std::nullopt,
    });
  }

  ~BufPoolImpl() = default;

  [[nodiscard]] bool HasPage(const PageId& page) const {
    const std::optional<BufferedPage> p = this->GetPage(page);
    return p.has_value();
  }

  [[nodiscard]] std::optional<BufferedPage> GetPage(
      const PageId& page_id) const {
    TrieNode& node = this->trie_find_bucket(hash_func_(page_id), this->bits_);

    for (BufferedPage& page : node.bucket.value()) {
      if (page.id == page_id) {
        this->evictor->MarkUsed(page.id);
        return std::make_optional(page);
      }
    }

    return std::nullopt;
  }

  void PutPage(const PageId& page_id, const PageType type,
               const Buffer contents) {
    uint32_t hash = hash_func_(page_id);

    TrieNode& node = BufPoolImpl::trie_find_bucket(hash, this->bits_);
    auto page = BufferedPage{
        .id = page_id,
        .type = type,
        .contents = contents,
    };
    bool removed_one = BufPoolImpl::trie_put_in_bucket(node, page);
    if (!removed_one) {
      this->elements_ += 1;
      this->consider_resizing();
      this->consider_evicting(page);
    }
  }

  [[nodiscard]] std::string DebugPrint(uint32_t bit_length) const {
    std::ostringstream s;
    s << "max: " << std::to_string(this->max_elements_) << "\n";
    s << "elements: " << std::to_string(this->elements_) << "\n";
    s << "bits: " << std::to_string(this->bits_) << "\n";
    s << "capacity: " << std::to_string(this->capacity_) << "\n";

    s << trie_print(*this->root_, "", 0, bit_length);

    return s.str();
  }
};

BufPool::BufPool(const BufPoolTuning tuning, std::unique_ptr<Evictor> evictor,
                 uint32_t (*hash)(const PageId&))
    : impl_(std::make_unique<BufPoolImpl>(tuning.initial_elements,
                                          tuning.max_elements,
                                          std::move(evictor), hash)) {}

BufPool::~BufPool() = default;

bool BufPool::HasPage(PageId& page) const { return this->impl_->HasPage(page); }

std::optional<BufferedPage> BufPool::GetPage(PageId& page) const {
  return this->impl_->GetPage(page);
}

void BufPool::PutPage(PageId& page, const PageType type,
                      const Buffer contents) {
  return this->impl_->PutPage(page, type, contents);
}

std::string BufPool::DebugPrint(uint32_t bit_length) {
  return this->impl_->DebugPrint(bit_length);
}

std::string BufPool::DebugPrint() { return this->impl_->DebugPrint(32); }