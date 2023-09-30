#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "memtable.hpp"

#include <assert.h>

std::string repeat(const std::string& input, unsigned num)
{
  std::string ret;
  ret.reserve(input.size() * num);
  while (num--) {
    ret += input;
  }
  return ret;
}

RbNode nil_sentinel;

RbNode::RbNode(K key, V value)
    : _key(key)
    , is_nil(false)
{
  this->_data = value;

  this->_color = black;
  this->_parent = this;
  this->_left = this;
  this->_right = this;
}

RbNode::RbNode()
    : _key(0)
    , is_nil(true)
{
  this->_data = 0;
  this->_color = black;

  this->_parent = this;
  this->_left = this;
  this->_right = this;
}

const bool RbNode::is_none() const
{
  return this->is_nil;
}

const bool RbNode::is_some() const
{
  return !this->is_nil;
}

void RbNode::set_parent(RbNode* parent)
{
  this->_parent = parent;
}

void RbNode::set_left(RbNode* left)
{
  if (this->is_nil) {
    return;
  }

  this->_left = left;
}

void RbNode::set_right(RbNode* right)
{
  if (this->is_nil) {
    return;
  }

  this->_right = right;
}

void RbNode::set_color(const Color color)
{
  if (this->is_nil) {
    return;
  }

  this->_color = color;
}

const K& RbNode::key() const
{
  return this->_key;
}

V* RbNode::value() const
{
  return const_cast<V*>(&this->_data);
}

V RbNode::replace_value(V new_value)
{
  V old_data = this->_data;
  this->_data = new_value;
  return old_data;
}

RbNode* RbNode::parent() const
{
  return this->_parent;
}

RbNode* RbNode::left() const
{
  if (this->is_nil) {
    return &nil_sentinel;
  }
  return this->_left;
}

RbNode* RbNode::right() const
{
  if (this->is_nil) {
    return &nil_sentinel;
  }
  return this->_right;
}

Color RbNode::color() const
{
  if (this->is_nil) {
    return black;
  }

  return this->_color;
}

bool RbNode::operator<(const RbNode& other) const
{
  return this->_key < other._key;
}

bool RbNode::operator<=(const RbNode& other) const
{
  return this->_key <= other._key;
}

bool RbNode::operator>(const RbNode& other) const
{
  return this->_key > other._key;
}

bool RbNode::operator>=(const RbNode& other) const
{
  return this->_key >= other._key;
}

bool RbNode::operator==(const RbNode& other) const
{
  if ((this->is_none() && other.is_none())
      || (this->is_some() && other.is_some() && this->_key == other._key))
  {
    return true;
  }

  return false;
}

std::string RbNode::print() const
{
  return this->print(1);
}

std::string RbNode::print(int depth) const
{
  if (this->is_nil) {
    return "{NULL}\n";
  }

  std::string k = std::to_string(this->_key);
  std::string v = std::to_string(this->_data);

  std::string offset = repeat("=", 4 * depth);
  std::ostringstream os;
  os << "(" << (this->_color == black ? "b" : "r") << ")[" + k + "] " + v + "\n"
     << offset + this->_left->print(depth + 1)
     << offset + this->_right->print(depth + 1);
  return os.str();
}

MemTable::MemTable(unsigned long long capacity)
{
  this->capacity = capacity;
  this->size = 0;
  this->root = &nil_sentinel;
}

std::string MemTable::Print() const
{
  return this->root->print();
}

char* MemTableFullException::what()
{
  return (char*)"MemTable is full! Can not insert any more elements, please flush "
         "the contents into a file!";
}

V* MemTable::Get(const K key) const
{
  RbNode* node = this->rb_search(this->root, key);
  if (node->is_some()) {
    return node->value();
  } else {
    return NULL;
  }
}

std::optional<V> MemTable::Put(const K key, const V value)
{
  RbNode* preexisting_node = this->rb_search(this->root, key);
  if (preexisting_node->is_some()) {
    return std::make_optional(preexisting_node->replace_value(value));
  }

  if (this->size == this->capacity) {
    throw MemTableFullException();
  }

  RbNode* node = new RbNode(key, value);
  this->rb_insert(node);
  this->size++;

  return std::nullopt;
}

std::vector<std::pair<K, V>> MemTable::Scan(const K lower_bound,
                                            const K upper_bound) const
{
  return this->rb_in_order(this->root, lower_bound, upper_bound);
}

V* MemTable::Delete(const K key)
{
  RbNode* node = this->rb_search(this->root, key);

  this->rb_delete(node);

  if (node->is_none()) {
    return NULL;
  } else {
    return node->value();
  }

  delete node;
}

std::vector<std::pair<K, V>> MemTable::rb_in_order(RbNode* node,
                                                   const K lower_bound,
                                                   const K upper_bound) const
{
  assert(node->is_some());
  std::vector<std::pair<K, V>> l;

  if (node->key() < lower_bound || node->key() > upper_bound) {
    return l;
  }

  if (node->left()->is_some()) {
    std::vector<std::pair<K, V>> left_vec =
        this->rb_in_order(node->left(), lower_bound, upper_bound);
    l.insert(l.end(), left_vec.begin(), left_vec.end());
  }

  const std::pair<K, V> elem = std::make_pair(node->key(), *node->value());
  l.push_back(elem);

  if (node->right()->is_some()) {
    std::vector<std::pair<K, V>> right_vec =
        this->rb_in_order(node->right(), lower_bound, upper_bound);
    l.insert(l.end(), right_vec.begin(), right_vec.end());
  }

  return l;
}

RbNode* MemTable::rb_search(RbNode* node, const K key) const
{
  while (node->is_some() && key != node->key()) {
    if (key < node->key()) {
      node = node->left();
    } else {
      node = node->right();
    }
  }
  return node;
}

RbNode* MemTable::rb_minimum(RbNode* node) const
{
  while (node->left()->is_some()) {
    node = node->left();
  }
  return node;
}

RbNode* MemTable::rb_maximum(RbNode* node) const
{
  while (node->right()->is_some()) {
    node = node->right();
  }
  return node;
}

void MemTable::rb_delete(RbNode* z)
{
  RbNode* y = z;
  Color y_original_color = y->color();

  RbNode* x;
  if (z->left()->is_none()) {
    x = z->right();
    this->rb_transplant(z, z->right());
  } else if (z->right()->is_none()) {
    x = z->left();
    this->rb_transplant(z, z->left());
  } else {
    y = this->rb_minimum(z->right());
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

  if (y_original_color == black) {
    this->rb_delete_fixup(x);
  }
}

void MemTable::rb_delete_fixup(RbNode* x)
{
  while (x->is_some() && x->color() == black) {
    if (*x == *x->parent()->left()) {
      RbNode* w = x->parent()->right();
      if (w->color() == red) {
        w->set_color(black);
        x->parent()->set_color(red);
        this->rb_left_rotate(x->parent());
        w = x->parent()->right();
      }

      if (w->left()->color() == black && w->right()->color() == black) {
        w->set_color(red);
        x = x->parent();
      } else if (w->right()->color() == black) {
        w->left()->set_color(black);
        w->set_color(red);
        this->rb_right_rotate(w);
        w = x->parent()->right();
      }

      w->set_color(x->parent()->color());
      x->parent()->set_color(black);
      w->right()->set_color(black);
      this->rb_left_rotate(x->parent());
      x = this->root;
    } else {
      RbNode* w = x->parent()->left();
      if (w->color() == red) {
        w->set_color(black);
        x->parent()->set_color(red);
        this->rb_right_rotate(x->parent());
        w = x->parent()->left();
      }

      if (w->right()->color() == black && w->left()->color() == black) {
        w->set_color(red);
        x = x->parent();
      } else if (w->left()->color() == black) {
        w->right()->set_color(black);
        w->set_color(red);
        this->rb_left_rotate(w);
        w = x->parent()->left();
      }

      w->set_color(x->parent()->color());
      x->parent()->set_color(black);
      w->left()->set_color(black);
      this->rb_right_rotate(x->parent());
      x = this->root;
    }
  }

  x->set_color(black);
}

void MemTable::rb_transplant(RbNode* u, RbNode* v)
{
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

void MemTable::rb_left_rotate(RbNode* x)
{
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

void MemTable::rb_right_rotate(RbNode* x)
{
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

void MemTable::rb_insert(RbNode* z)
{
  RbNode* y = &nil_sentinel;
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
  z->set_left(&nil_sentinel);
  z->set_right(&nil_sentinel);
  z->set_color(red);

  this->rb_insert_fixup(z);
}

void MemTable::rb_insert_fixup(RbNode* z)
{
  while (z->parent()->color() == red) {
    if (*z->parent() == *z->parent()->parent()->left()) {
      RbNode* y = z->parent()->parent()->right();
      if (y->color() == red) {
        z->parent()->set_color(black);
        y->set_color(black);
        z->parent()->parent()->set_color(red);
        z = z->parent()->parent();
      } else {
        if (*z == *z->parent()->right()) {
          z = z->parent();
          this->rb_left_rotate(z);
        }
        z->parent()->set_color(black);
        z->parent()->parent()->set_color(red);
        this->rb_right_rotate(z->parent()->parent());
      }
    } else {
      RbNode* y = z->parent()->parent()->left();
      if (y->color() == red) {
        z->parent()->set_color(black);
        y->set_color(black);
        z->parent()->parent()->set_color(red);
        z = z->parent()->parent();
      } else {
        if (*z == *z->parent()->left()) {
          z = z->parent();
          this->rb_right_rotate(z);
        }

        z->parent()->set_color(black);
        z->parent()->parent()->set_color(red);
        this->rb_left_rotate(z->parent()->parent());
      }
    }
  }

  this->root->set_color(black);
}
