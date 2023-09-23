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
  this->key = key;
  this->data = data;
}

template<typename K, typename V>
void RbNode<K, V>::set_parent(const RbNode<K, V>& parent)
{
  this->parent = parent;
}

template<typename K, typename V>
std::optional<std::reference_wrapper<RbNode<K, V>>> RbNode<K, V>::get_parent()
    const
{
  return this->parent;
}

template<typename K, typename V>
std::string RbNode<K, V>::print() const
{
  std::string left_display;
  if (this->left) {
    left_display = this->left->get().print();
  } else {
    left_display = "{NULL}";
  }

  std::string right_display;
  if (this->right) {
    right_display = this->right->get().print();
  } else {
    right_display = "{NULL}";
  }

  std::ostringstream os;
  os << "[" + std::to_string(this->key) + "] " + this->data + "\n"
     << "  " + left_display + "\n"
     << "  " + right_display + "\n";
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
  return;
}

template<typename K, typename V>
std::optional<V> MemTable<K, V>::Delete(const K key)
{
  return std::nullopt;
}

template class MemTable<int, std::string>;
