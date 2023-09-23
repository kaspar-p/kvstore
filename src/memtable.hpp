#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

enum Color
{
  red,
  black
};

template<typename K, typename V>
class RbNode
{
public:
  RbNode(K key, V value);
  RbNode();

  Color color() const;
  void set_color(const Color color);

  const bool is_some() const;
  const bool is_none() const;

  RbNode* parent() const;
  RbNode* left() const;
  RbNode* right() const;

  void set_parent(RbNode* parent);
  void set_left(RbNode* left);
  void set_right(RbNode* right);

  bool operator<(const RbNode& other) const;
  bool operator<=(const RbNode& other) const;
  bool operator>(const RbNode& other) const;
  bool operator>=(const RbNode& other) const;
  bool operator==(const RbNode& other) const;

  std::string print() const;

private:
  bool is_nil;
  K _key;
  V _data;
  Color _color;
  RbNode* _parent;
  RbNode* _left;
  RbNode* _right;
};

template<typename K, typename V>
class MemTable
{
private:
  int memtable_size;
  RbNode<K, V>* root;
  void rb_transplant(RbNode<K, V>* u, RbNode<K, V>* v);
  void rb_left_rotate(RbNode<K, V>* node);
  void rb_right_rotate(RbNode<K, V>* node);
  void rb_insert(RbNode<K, V>* node);
  void rb_insert_fixup(RbNode<K, V>* node);
  void rb_delete(RbNode<K, V>& node);
  void rb_delete_fixup(RbNode<K, V>& node);

public:
  MemTable(int memtable_size);
  std::string Print() const;
  std::optional<std::reference_wrapper<V>> Get(const K key) const;
  void Put(const K key, const V value);
  std::optional<V> Delete(const K key);
};
