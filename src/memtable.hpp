#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

template<typename K, typename V>
class RbNode
{
public:
  RbNode(K key, V value);
  void set_parent(const RbNode& parent);
  std::optional<std::reference_wrapper<RbNode>> get_parent() const;
  std::string print() const;

private:
  K key;
  V data;
  std::optional<std::reference_wrapper<RbNode>> parent;
  std::optional<std::reference_wrapper<RbNode>> left;
  std::optional<std::reference_wrapper<RbNode>> right;
};

template<typename K, typename V>
class MemTable
{
private:
  int memtable_size;
  std::unique_ptr<RbNode<K, V>> root;

public:
  MemTable(int memtable_size);
  std::string Print() const;
  std::optional<std::reference_wrapper<V>> Get(const K key) const;
  void Put(const K key, const V value);
  std::optional<V> Delete(const K key);
};
