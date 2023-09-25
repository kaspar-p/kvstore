#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "memtable.hpp"

std::string repeat(const std::string& input, unsigned num)
{
  std::string ret;
  ret.reserve(input.size() * num);
  while (num--) {
    ret += input;
  }
  return ret;
}

template<typename K, typename V>
RbNode<K, V> nil_sentinel;

template<typename K, typename V>
RbNode<K, V>::RbNode(K key, V value)
    : _key(key)
    , _data(value)
    , is_nil(false)
{
  this->_color = black;
  this->_parent = this;
  this->_left = this;
  this->_right = this;
}

template<typename K, typename V>
RbNode<K, V>::RbNode()
    : _key(0)
    , _data("")
    , is_nil(true)
{
  this->_color = black;

  this->_parent = this;
  this->_left = this;
  this->_right = this;
}

template<typename K, typename V>
const bool RbNode<K, V>::is_none() const
{
  return this->is_nil;
}

template<typename K, typename V>
const bool RbNode<K, V>::is_some() const
{
  return !this->is_nil;
}

template<typename K, typename V>
void RbNode<K, V>::set_parent(RbNode<K, V>* parent)
{
  this->_parent = parent;
}

template<typename K, typename V>
void RbNode<K, V>::set_left(RbNode<K, V>* left)
{
  if (this->is_nil) {
    return;
  }

  this->_left = left;
}

template<typename K, typename V>
void RbNode<K, V>::set_right(RbNode<K, V>* right)
{
  if (this->is_nil) {
    return;
  }

  this->_right = right;
}

template<typename K, typename V>
void RbNode<K, V>::set_color(const Color color)
{
  if (this->is_nil) {
    return;
  }

  this->_color = color;
}

template<typename K, typename V>
const K& RbNode<K, V>::key() const
{
  return this->_key;
}

template<typename K, typename V>
const V* RbNode<K, V>::value() const
{
  return &this->_data;
}

template<typename K, typename V>
RbNode<K, V>* RbNode<K, V>::parent() const
{
  return this->_parent;
}

template<typename K, typename V>
RbNode<K, V>* RbNode<K, V>::left() const
{
  if (this->is_nil) {
    return &nil_sentinel<K, V>;
  }
  return this->_left;
}

template<typename K, typename V>
RbNode<K, V>* RbNode<K, V>::right() const
{
  if (this->is_nil) {
    return &nil_sentinel<K, V>;
  }
  return this->_right;
}

template<typename K, typename V>
Color RbNode<K, V>::color() const
{
  if (this->is_nil) {
    return black;
  }

  return this->_color;
}

template<typename K, typename V>
bool RbNode<K, V>::operator<(const RbNode& other) const
{
  return this->_key < other._key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator<=(const RbNode& other) const
{
  return this->_key <= other._key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator>(const RbNode& other) const
{
  return this->_key > other._key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator>=(const RbNode& other) const
{
  return this->_key >= other._key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator==(const RbNode& other) const
{
  if ((this->is_none() && other.is_none())
      || (this->is_some() && other.is_some() && this->_key == other._key))
  {
    return true;
  }

  return false;
}

template<typename K, typename V>
std::string RbNode<K, V>::print() const
{
  return this->print(1);
}

template<typename K, typename V>
std::string RbNode<K, V>::print(int depth) const
{
  if (this->is_nil) {
    return "{NULL}\n";
  }

  std::string offset = repeat("-", 4 * depth);
  std::ostringstream os;
  os << "(" << (this->_color == black ? "b" : "r")
     << ")[" + std::to_string(this->_key) + "] " + this->_data + "\n"
     << offset + this->_left->print(depth + 1)
     << offset + this->_right->print(depth + 1);
  return os.str();
}

template<typename K, typename V>
MemTable<K, V>::MemTable(int memtable_size)
{
  this->memtable_size = memtable_size;
  this->root = &nil_sentinel<K, V>;
}

template<typename K, typename V>
std::string MemTable<K, V>::Print() const
{
  return this->root->print();
}

template<typename K, typename V>
const V* MemTable<K, V>::Get(const K key) const
{
  RbNode<K, V>* node = this->rb_search(this->root, key);
  if (node->is_some()) {
    return node->value();
  } else {
    return NULL;
  }
}

template<typename K, typename V>
void MemTable<K, V>::Put(const K key, const V value)
{
  RbNode<K, V>* node = new RbNode<K, V>(key, value);
  this->rb_insert(node);
}

template<typename K, typename V>
const V* MemTable<K, V>::Delete(const K key)
{
  RbNode<K, V>* node = this->rb_search(this->root, key);

  this->rb_delete(node);

  if (node->is_none()) {
    return NULL;
  } else {
    return node->value();
  }

  delete node;
}

template<typename K, typename V>
RbNode<K, V>* MemTable<K, V>::rb_search(RbNode<K, V>* node, const K key) const
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

template<typename K, typename V>
RbNode<K, V>* MemTable<K, V>::rb_minimum(RbNode<K, V>* node) const
{
  while (node->left()->is_some()) {
    node = node->left();
  }
  return node;
}

template<typename K, typename V>
RbNode<K, V>* MemTable<K, V>::rb_maximum(RbNode<K, V>* node) const
{
  while (node->right()->is_some()) {
    node = node->right();
  }
  return node;
}

template<typename K, typename V>
void MemTable<K, V>::rb_delete(RbNode<K, V>* z)
{
  RbNode<K, V>* y = z;
  Color y_original_color = y->color();

  RbNode<K, V>* x;
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

template<typename K, typename V>
void MemTable<K, V>::rb_delete_fixup(RbNode<K, V>* x)
{
  while (x->is_some() && x->color() == black) {
    if (*x == *x->parent()->left()) {
      RbNode<K, V>* w = x->parent()->right();
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
      RbNode<K, V>* w = x->parent()->left();
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

template<typename K, typename V>
void MemTable<K, V>::rb_transplant(RbNode<K, V>* u, RbNode<K, V>* v)
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

template<typename K, typename V>
void MemTable<K, V>::rb_left_rotate(RbNode<K, V>* x)
{
  if (x->is_none()) {
    return;
  }

  RbNode<K, V>* y = x->right();
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

template<typename K, typename V>
void MemTable<K, V>::rb_right_rotate(RbNode<K, V>* x)
{
  if (x->is_none()) {
    return;
  }

  auto y = x->left();
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

template<typename K, typename V>
void MemTable<K, V>::rb_insert(RbNode<K, V>* z)
{
  RbNode<K, V>* y = &nil_sentinel<K, V>;
  RbNode<K, V>* x = this->root;
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
  z->set_left(&nil_sentinel<K, V>);
  z->set_right(&nil_sentinel<K, V>);
  z->set_color(red);

  this->rb_insert_fixup(z);
}

template<typename K, typename V>
void MemTable<K, V>::rb_insert_fixup(RbNode<K, V>* z)
{
  while (z->parent()->color() == red) {
    if (*z->parent() == *z->parent()->parent()->left()) {
      RbNode<K, V>* y = z->parent()->parent()->right();
      if (y->color() == red) {
        z->parent()->set_color(black);
        y->set_color(black);
        z->parent()->parent()->set_color(red);
        z = z->parent()->parent();
      } else if (*z == *z->parent()->right()) {
        z = z->parent();
        this->rb_left_rotate(z);
      }
      z->parent()->set_color(black);
      z->parent()->parent()->set_color(red);
      this->rb_right_rotate(z->parent()->parent());
    } else {
      RbNode<K, V>* y = z->parent()->parent()->left();
      if (y->color() == red) {
        z->parent()->set_color(black);
        y->set_color(black);
        z->parent()->parent()->set_color(red);
        z = z->parent()->parent();
      } else if (*z == *z->parent()->left()) {
        z = z->parent();
        this->rb_right_rotate(z);
      }
      z->parent()->set_color(black);
      z->parent()->parent()->set_color(red);
      this->rb_left_rotate(z->parent()->parent());
    }
  }

  this->root->set_color(black);
}

template class MemTable<int, std::string>;
