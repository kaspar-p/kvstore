#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "memtable.hpp"

template<typename K, typename V>
RbNode<K, V>::RbNode(K key, V value)
{
  this->is_nil = false;
  this->_key = key;
  this->_data = value;

  this->_parent = new RbNode();
  this->_left = new RbNode();
  this->_right = new RbNode();
}

template<typename K, typename V>
RbNode<K, V>::RbNode()
{
  this->is_nil = true;
  this->_color = black;

  this->_parent = new RbNode();
  this->_left = new RbNode();
  this->_right = new RbNode();
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
RbNode<K, V>* RbNode<K, V>::parent() const
{
  return this->_parent;
}

template<typename K, typename V>
RbNode<K, V>* RbNode<K, V>::left() const
{
  if (this->is_nil) {
    return new RbNode();
  }
  return this->_left;
}

template<typename K, typename V>
RbNode<K, V>* RbNode<K, V>::right() const
{
  if (this->is_nil) {
    return new RbNode();
  }
  return this->_right;
}

template<typename K, typename V>
Color RbNode<K, V>::color() const
{
  return this->_color;
}

template<typename K, typename V>
bool RbNode<K, V>::operator<(const RbNode& other) const
{
  return this->_key < other->_key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator<=(const RbNode& other) const
{
  return this->_key <= other->_key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator>(const RbNode& other) const
{
  return this->_key > other->_key;
}

template<typename K, typename V>
bool RbNode<K, V>::operator>=(const RbNode& other) const
{
  return this->_key >= other->_key;
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
  if (this->is_nil) {
    return "{NULL}";
  }

  std::ostringstream os;
  os << "[" + std::to_string(this->_key) + "] " + this->_data + "\n"
     << "  " + this->_left->print() + "\n"
     << "  " + this->_right->print() + "\n";
  return os.str();
}

template<typename K, typename V>
MemTable<K, V>::MemTable(int memtable_size)
{
  this->memtable_size = memtable_size;
}

template<typename K, typename V>
std::string MemTable<K, V>::Print() const
{
  if (this->root) {
    return this->root->print();
  }
  return "{NULL}";
}

template<typename K, typename V>
std::optional<std::reference_wrapper<V>> MemTable<K, V>::Get(const K key) const
{
  return std::nullopt;
}

template<typename K, typename V>
void MemTable<K, V>::Put(const K key, const V value)
{
  RbNode<K, V>* node = new RbNode<K, V>(key, value);
  this->rb_insert(node);
}

template<typename K, typename V>
std::optional<V> MemTable<K, V>::Delete(const K key)
{
  return std::nullopt;
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
  auto y = x->right();
  x->set_right(y->left());
  if (y->left()->is_some()) {
    y->left()->set_parent(x);
  }
  y->set_parent(x->parent());
  if (x->parent()->is_none()) {
    this->root = y;
  } else if (x == x->parent()->left()) {
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
  auto y = x->left();
  x->set_left(y->right());
  if (y->right()->is_some()) {
    y->right()->set_parent(x);
  }
  y->set_parent(x->parent());
  if (x->parent()->is_none()) {
    this->root = y;
  } else if (x == x->parent()->right()) {
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
  RbNode<K, V>* y = new RbNode<K, V>();
  RbNode<K, V>* x = this->root;
  while (x->is_some()) {
    y = x;
    if (z < x) {
      x = x->left();
    } else {
      x = x->right();
    }
  }
  z->set_parent(y);
  if (y->is_none()) {
    this->root = y;
  } else if (z < y) {
    y->set_left(z);
  } else {
    y->set_right(z);
  }
  z->set_left(new RbNode<K, V>());
  z->set_right(new RbNode<K, V>());
  z->set_color(red);
  this->rb_insert_fixup(z);
}

template<typename K, typename V>
void MemTable<K, V>::rb_insert_fixup(RbNode<K, V>* z)
{
  while (z->parent()->color() == red) {
    if (z->parent() == z->parent()->parent()->left()) {
      RbNode<K, V>* y = z->parent()->parent()->right();
      if (y->color() == red) {
        z->parent()->set_color(black);
        y->set_color(black);
        z->parent()->parent()->set_color(red);
        z = z->parent()->parent();
      } else if (z == z->parent()->right()) {
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
      } else if (z == z->parent()->left()) {
        z = z->parent();
        this->rb_right_rotate(z);
      }
      z->parent()->set_color(black);
      z->parent()->parent()->set_color(red);
      this->rb_left_rotate(z->parent()->parent());
    }
  }
}

template class MemTable<int, std::string>;
