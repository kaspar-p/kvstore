#include "memtable.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "dbg.hpp"

enum Color { kRed = 0, kBlack = 1 };

class RbNode;
RbNode* nil_sentinel_node = nullptr;
RbNode* nil_sentinel();

class RbNode {
 public:
  RbNode(K key, V value) : key_(key), is_nil_(false) {
    this->data_ = value;

    this->color_ = kBlack;
    this->parent_ = this;
    this->left_ = this;
    this->right_ = this;
  }
  RbNode() : key_(0), is_nil_(true) {
    this->data_ = 0;
    this->color_ = kBlack;

    this->parent_ = this;
    this->left_ = this;
    this->right_ = this;
  }

  [[nodiscard]] Color color() const {
    if (this->is_nil_) {
      return kBlack;
    }

    return this->color_;
  };
  void set_color(const Color color) {
    if (this->is_nil_) {
      return;
    }

    this->color_ = color;
  };

  [[nodiscard]] bool is_some() const { return !this->is_nil_; }
  [[nodiscard]] bool is_none() const { return this->is_nil_; }

  [[nodiscard]] const K& key() const { return this->key_; }
  [[nodiscard]] V* value() const { return const_cast<V*>(&this->data_); }

  V replace_value(V new_value) {
    V old_data = this->data_;
    this->data_ = new_value;
    return old_data;
  }

  [[nodiscard]] RbNode* parent() const { return this->parent_; }
  [[nodiscard]] RbNode* left() const {
    if (this->is_nil_) {
      return nil_sentinel();
    }
    return this->left_;
  }
  [[nodiscard]] RbNode* right() const {
    if (this->is_nil_) {
      return nil_sentinel();
    }
    return this->right_;
  }

  void set_parent(RbNode* parent) { this->parent_ = parent; };
  void set_left(RbNode* left) {
    if (this->is_nil_) {
      return;
    }

    this->left_ = left;
  };
  void set_right(RbNode* right) {
    if (this->is_nil_) {
      return;
    }

    this->right_ = right;
  };

  bool operator<(const RbNode& other) const { return this->key_ < other.key_; }
  bool operator<=(const RbNode& other) const {
    return this->key_ <= other.key_;
  }
  bool operator>(const RbNode& other) const { return this->key_ > other.key_; }
  bool operator>=(const RbNode& other) const {
    return this->key_ >= other.key_;
  }
  bool operator==(const RbNode& other) const {
    return ((this->is_none() && other.is_none()) ||
            (this->is_some() && other.is_some() && this->key_ == other.key_));
  }

  [[nodiscard]] std::string print() const { return this->print(1); }

 private:
  const bool is_nil_;
  const K key_;
  V data_;
  Color color_;
  RbNode* parent_;
  RbNode* left_;
  RbNode* right_;

  [[nodiscard]] std::string print(int depth) const {
    if (this->is_nil_) {
      return std::string("{NULL}\n");
    }

    std::string k = std::to_string(this->key_);
    std::string v = std::to_string(this->data_);

    std::string offset = repeat("=", 4 * depth);
    std::ostringstream os;
    os << "(" << (this->color_ == kBlack ? "b" : "r")
       << ")[" + k + "] " + v + "\n"
       << offset + this->left_->print(depth + 1)
       << offset + this->right_->print(depth + 1);
    return os.str();
  }
};

RbNode* nil_sentinel() {
  if (nil_sentinel_node == nullptr) {
    nil_sentinel_node = new RbNode();
  }
  return nil_sentinel_node;
}

const char* MemTableFullException::what() const noexcept {
  return "MemTable is full! Can not insert any more elements, please flush "
         "the contents into a file!";
}

class MemTable::MemTableImpl {
 private:
  uint64_t capacity;
  uint64_t size_;
  std::optional<K> least_key_;
  std::optional<K> most_key_;
  RbNode* root;

  /**
   * @brief Return a vector of pairs, sorted. All pairs (k, v) in
   * the vector are such that lower_bound < k < upper_bound, exclusive.
   *
   * @param subtree The beginning of the search, often the root.
   * @param lower_bound The lower bound on keys to return.
   * @param upper_bound The upper bound on keys to return.
   * @return std::vector<std::pair<K,V>> A list of ordered pairs.
   */
  std::vector<RbNode*> rb_in_order(RbNode* node, const K lower_bound,
                                   const K upper_bound) const {
    std::vector<RbNode*> l{};
    if (node->is_none()) {
      return l;
    }

    if (node->key() < lower_bound || node->key() > upper_bound) {
      return l;
    }

    if (node->left()->is_some()) {
      std::vector<RbNode*> left_vec =
          this->rb_in_order(node->left(), lower_bound, upper_bound);
      l.insert(l.end(), left_vec.begin(), left_vec.end());
    }

    l.push_back(node);

    if (node->right()->is_some()) {
      std::vector<RbNode*> right_vec =
          this->rb_in_order(node->right(), lower_bound, upper_bound);
      l.insert(l.end(), right_vec.begin(), right_vec.end());
    }

    return l;
  }

  /**
   * @brief Search the red-black tree for a node where node.key() == key.
   * Implementation described in my roommate's copy of CLRS.
   *
   * @param subtree The starting point of the search, often the root.
   * @param key The key to search for exact matches.
   * @return RbNode* The node with matching key.
   * If no such node, returns a nil sentinel.
   */
  static RbNode* rb_search(RbNode* node, const K key) {
    while (node->is_some() && key != node->key()) {
      if (key < node->key()) {
        node = node->left();
      } else {
        node = node->right();
      }
    }
    return node;
  }

  /**
   * @brief Find the node with minimum key in a subtree.
   * Symmetric with `rb_maximum`. If `subtree` is itself a nil sentinel,
   * this function returns a nil sentinel. Implementation described in my
   * roommate's copy of CLRS.
   *
   * @param subtree The starting point of the search, often the root.
   * @return RbNode* The resulting minimum node.
   * If there are no such nodes, returns a nil sentinel.
   */
  static RbNode* rb_minimum(RbNode* node) {
    while (node->left()->is_some()) {
      node = node->left();
    }
    return node;
  }

  /**
   * @brief Find the node with maximum key in a subtree.
   * Symmetric with `rb_minimum`. If `subtree` is itself a nil sentinel,
   * this function returns a nil sentinel. Implementation described in my
   * roommate's copy of CLRS.
   *
   * @param subtree The starting point of the search, often the root.
   * @return RbNode* The resulting maximum node.
   * If there are no such nodes, returns a nil sentinel.
   */
  static RbNode* rb_maximum(RbNode* node) {
    while (node->right()->is_some()) {
      node = node->right();
    }
    return node;
  }

  /**
   * @brief Delete a node from the red-black tree. Implementation described in
   * my roommate's copy of CLRS.
   *
   * @param node The node to delete.
   */
  void rb_delete(RbNode* z) {
    RbNode* y = z;
    Color y_original_color = y->color();

    RbNode* x = nullptr;
    if (z->left()->is_none()) {
      x = z->right();
      this->rb_transplant(z, z->right());
    } else if (z->right()->is_none()) {
      x = z->left();
      this->rb_transplant(z, z->left());
    } else {
      y = rb_minimum(z->right());
      y_original_color = y->color();
      x = y->right();
      if (*y->parent() == *z) {
        x->set_parent(y);
      } else {
        this->rb_transplant(y, y->right());
        y->set_right(z->right());
        y->right()->set_parent(y);
      }
      this->rb_transplant(z, y);
      y->set_left(z->left());
      y->left()->set_parent(y);
      y->set_color(z->color());
    }

    if (y_original_color == kBlack) {
      this->rb_delete_fixup(x);
    }
  }

  /**
   * @brief Fix the balanced property of a red-black tree. Implementation
   * described in my roommate's copy of CLRS.
   *
   * @param node The node to fix the balancing property around.
   */
  void rb_delete_fixup(RbNode* x) {
    while (x->is_some() && x->color() == kBlack) {
      if (*x == *x->parent()->left()) {
        RbNode* w = x->parent()->right();
        if (w->color() == kRed) {
          w->set_color(kBlack);
          x->parent()->set_color(kRed);
          this->rb_left_rotate(x->parent());
          w = x->parent()->right();
        }

        if (w->left()->color() == kBlack && w->right()->color() == kBlack) {
          w->set_color(kRed);
          x = x->parent();
        } else if (w->right()->color() == kBlack) {
          w->left()->set_color(kBlack);
          w->set_color(kRed);
          this->rb_right_rotate(w);
          w = x->parent()->right();
        }

        w->set_color(x->parent()->color());
        x->parent()->set_color(kBlack);
        w->right()->set_color(kBlack);
        this->rb_left_rotate(x->parent());
        x = this->root;
      } else {
        RbNode* w = x->parent()->left();
        if (w->color() == kRed) {
          w->set_color(kBlack);
          x->parent()->set_color(kRed);
          this->rb_right_rotate(x->parent());
          w = x->parent()->left();
        }

        if (w->right()->color() == kBlack && w->left()->color() == kBlack) {
          w->set_color(kRed);
          x = x->parent();
        } else if (w->left()->color() == kBlack) {
          w->right()->set_color(kBlack);
          w->set_color(kRed);
          this->rb_left_rotate(w);
          w = x->parent()->left();
        }

        w->set_color(x->parent()->color());
        x->parent()->set_color(kBlack);
        w->left()->set_color(kBlack);
        this->rb_right_rotate(x->parent());
        x = this->root;
      }
    }

    x->set_color(kBlack);
  }

  void rb_transplant(RbNode* u, RbNode* v) {
    if (u->parent()->is_none()) {
      this->root = v;
    } else if (u == u->parent()->left()) {
      u->parent()->set_left(v);
    } else {
      u->parent()->set_right(v);
    }

    if (v->is_some()) {
      v->set_parent(u->parent());
    }
  }

  /**
   * @brief Rotate a red-black tree _left_ around a node. Does nothing if the
   * node is a nil sentinel. Implementation described in my roommate's copy of
   * CLRS.
   *
   * @param node The node to rotate around.
   */
  void rb_left_rotate(RbNode* x) {
    if (x->is_none()) {
      return;
    }

    RbNode* y = x->right();
    x->set_right(y->left());
    if (y->left()->is_some()) {
      y->left()->set_parent(x);
    }
    y->set_parent(x->parent());
    if (x->parent()->is_none()) {
      this->root = y;
    } else if (*x == *x->parent()->left()) {
      x->parent()->set_left(y);
    } else {
      x->parent()->set_right(y);
    }

    y->set_left(x);
    x->set_parent(y);
  }

  /**
   * @brief Rotate a red-black tree _right_ around a node. Does nothing if the
   * node is a nil sentinel. Implementation described in my roommate's copy of
   * CLRS.
   *
   * @param node The node to rotate around.
   */
  void rb_right_rotate(RbNode* x) {
    if (x->is_none()) {
      return;
    }

    RbNode* y = x->left();
    x->set_left(y->right());
    if (y->right()->is_some()) {
      y->right()->set_parent(x);
    }
    y->set_parent(x->parent());
    if (x->parent()->is_none()) {
      this->root = y;
    } else if (*x == *x->parent()->right()) {
      x->parent()->set_right(y);
    } else {
      x->parent()->set_left(y);
    }

    y->set_right(x);
    x->set_parent(y);
  }

  /**
   * @brief Insert a node into the red-black tree. Implementation described in
   * my roommate's copy of CLRS.
   *
   * @param node The node to insert.
   */
  void rb_insert(RbNode* z) {
    RbNode* y = nil_sentinel();
    RbNode* x = this->root;
    while (x->is_some()) {
      y = x;
      if (*z < *x) {
        x = x->left();
      } else {
        x = x->right();
      }
    }

    z->set_parent(y);
    if (y->is_none()) {
      this->root = z;
    } else if (*z < *y) {
      y->set_left(z);
    } else {
      y->set_right(z);
    }
    z->set_left(nil_sentinel());
    z->set_right(nil_sentinel());
    z->set_color(kRed);

    this->rb_insert_fixup(z);
  }

  /**
   * @brief Fix the red-black tree balanced property. Implementation described
   * in my roommate's copy of CLRS.
   *
   * @param node The node to fix around.
   */
  void rb_insert_fixup(RbNode* z) {
    while (z->parent()->color() == kRed) {
      if (*z->parent() == *z->parent()->parent()->left()) {
        RbNode* y = z->parent()->parent()->right();
        if (y->color() == kRed) {
          z->parent()->set_color(kBlack);
          y->set_color(kBlack);
          z->parent()->parent()->set_color(kRed);
          z = z->parent()->parent();
        } else {
          if (*z == *z->parent()->right()) {
            z = z->parent();
            this->rb_left_rotate(z);
          }
          z->parent()->set_color(kBlack);
          z->parent()->parent()->set_color(kRed);
          this->rb_right_rotate(z->parent()->parent());
        }
      } else {
        RbNode* y = z->parent()->parent()->left();
        if (y->color() == kRed) {
          z->parent()->set_color(kBlack);
          y->set_color(kBlack);
          z->parent()->parent()->set_color(kRed);
          z = z->parent()->parent();
        } else {
          if (*z == *z->parent()->left()) {
            z = z->parent();
            this->rb_right_rotate(z);
          }

          z->parent()->set_color(kBlack);
          z->parent()->parent()->set_color(kRed);
          this->rb_left_rotate(z->parent()->parent());
        }
      }
    }

    this->root->set_color(kBlack);
  }

 public:
  explicit MemTableImpl(uint64_t capacity) {
    this->capacity = capacity;
    this->size_ = 0;
    this->root = nil_sentinel();
  }

  MemTableImpl(const MemTableImpl& t) = default;
  MemTableImpl& operator=(const MemTableImpl& t) = default;

  void IncreaseCapacity(std::size_t capacity) {
    assert(this->capacity <= capacity);
    this->capacity = capacity;
  }

  [[nodiscard]] std::size_t GetCapacity() const { return this->capacity; }

  [[nodiscard]] std::string Print() const { return this->root->print(); }

  [[nodiscard]] V* Get(const K key) const {
    RbNode* node = rb_search(this->root, key);
    if (node->is_some()) {
      return node->value();
    }
    return nullptr;
  }

  std::optional<V> Put(const K key, const V value) {
    RbNode* preexisting_node = rb_search(this->root, key);
    if (preexisting_node->is_some()) {
      return std::make_optional(preexisting_node->replace_value(value));
    }

    if (this->size_ == this->capacity) {
      throw MemTableFullException();
    }

    if (!this->least_key_.has_value() || key < this->least_key_.value()) {
      this->least_key_ = key;
    }
    if (!this->most_key_ || key > this->most_key_.value()) {
      this->most_key_ = key;
    }

    auto* node = new RbNode(key, value);
    this->rb_insert(node);
    this->size_++;

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(const K lower_bound,
                                                  const K upper_bound) const {
    std::vector<RbNode*> nodes =
        this->rb_in_order(this->root, lower_bound, upper_bound);

    std::vector<std::pair<K, V>> pairs;
    std::transform(nodes.begin(), nodes.end(), std::back_inserter(pairs),
                   [](RbNode* elem) {
                     return std::make_pair(elem->key(), *elem->value());
                   });
    return pairs;
  }

  [[nodiscard]] std::unique_ptr<std::vector<std::pair<K, V>>> ScanAll() const {
    auto pairs = std::make_unique<std::vector<std::pair<K, V>>>();
    if (!this->least_key_.has_value() || !this->most_key_.has_value()) {
      return pairs;
    }

    std::vector<RbNode*> nodes = this->rb_in_order(
        this->root, this->least_key_.value(), this->most_key_.value());

    std::transform(nodes.begin(), nodes.end(), std::back_inserter(*pairs),
                   [](RbNode* elem) {
                     return std::make_pair(elem->key(), *elem->value());
                   });
    return pairs;
  }

  V* Delete(const K key) {
    RbNode* node = rb_search(this->root, key);

    this->rb_delete(node);

    V* ret = nullptr;
    if (node->is_some()) {
      ret = node->value();
    }
    delete node;

    return ret;
  }

  void Clear() {
    for (RbNode* node :
         this->rb_in_order(this->root, rb_minimum(this->root)->key(),
                           rb_maximum(this->root)->key())) {
      if (node == this->root) {
        this->root = nil_sentinel();
      }
      delete node;
    }

    this->size_ = 0;
  }
};

MemTable::MemTable(uint64_t capacity) {
  this->impl = std::make_unique<MemTableImpl>(capacity);
}

MemTable::MemTable(const MemTable& t) : impl(new MemTableImpl(*t.impl)) {}

MemTable& MemTable::operator=(const MemTable& t) {
  *this->impl = *t.impl;
  return *this;
}

void MemTable::IncreaseCapacity(std::size_t capacity) {
  return this->impl->IncreaseCapacity(capacity);
}

std::size_t MemTable::GetCapacity() const { return this->impl->GetCapacity(); }

std::string MemTable::Print() const { return this->impl->Print(); }

V* MemTable::Get(const K key) const { return this->impl->Get(key); }

std::optional<V> MemTable::Put(const K key, const V value) {
  return this->impl->Put(key, value);
}

std::vector<std::pair<K, V>> MemTable::Scan(const K lower_bound,
                                            const K upper_bound) const {
  return this->impl->Scan(lower_bound, upper_bound);
}

V* MemTable::Delete(const K key) { return this->impl->Delete(key); }

std::unique_ptr<std::vector<std::pair<K, V>>> MemTable::ScanAll() const {
  return this->impl->ScanAll();
}

void MemTable::Clear() { this->impl->Clear(); }

MemTable::~MemTable() {
  try {
    this->impl->Clear();
  } catch (std::exception& e) {
    std::cerr << "FAILED TO CLEAR MEMTABLE: " << e.what() << '\n';
  }
}
