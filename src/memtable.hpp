#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

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

  const bool is_some(void) const;
  const bool is_none(void) const;

  const K& key() const;
  V* value() const;
  V replace_value(V new_value);

  RbNode* parent(void) const;
  RbNode* left(void) const;
  RbNode* right(void) const;

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
  const bool is_nil;
  const K _key;
  V _data;
  Color _color;
  RbNode* _parent;
  RbNode* _left;
  RbNode* _right;
  std::string print(int depth) const;
};

class MemTableFullException : public std::exception
{
public:
  char* what();
};

template<typename K, typename V>
class MemTable
{
private:
  unsigned long capacity;
  unsigned long size;
  RbNode<K, V>* root;

  /**
   * @brief Find the node with minimum key in a subtree.
   * Symmetric with `rb_maximum`. If `subtree` is itself a nil sentinel,
   * this function returns a nil sentinel.
   *
   * @param subtree The starting point of the search, often the root.
   * @return RbNode<K, V>* The resulting minimum node.
   * If there are no such nodes, returns a nil sentinel.
   */
  RbNode<K, V>* rb_minimum(RbNode<K, V>* subtree) const;

  /**
   * @brief Find the node with maximum key in a subtree.
   * Symmetric with `rb_minimum`. If `subtree` is itself a nil sentinel,
   * this function returns a nil sentinel.
   *
   * @param subtree The starting point of the search, often the root.
   * @return RbNode<K, V>* The resulting maximum node.
   * If there are no such nodes, returns a nil sentinel.
   */
  RbNode<K, V>* rb_maximum(RbNode<K, V>* subtree) const;

  /**
   * @brief Search the tree for a node where node.key() == key.
   *
   * @param subtree The starting point of the search, often the root.
   * @param key The key to search for exact matches.
   * @return RbNode<K, V>* The node with matching key.
   * If no such node, returns a nil sentinel.
   */
  RbNode<K, V>* rb_search(RbNode<K, V>* subtree, const K key) const;

  /**
   * @brief Return a vector of pairs, sorted. All pairs (k, v) in
   * the vector are such that lower_bound < k < upper_bound, exclusive.
   *
   * @param subtree The beginning of the search, often the root.
   * @param lower_bound The lower bound on keys to return.
   * * @param upper_bound The upper bound on keys to return.
   * @return std::vector<std::pair<K,V>> A list of ordered pairs.
   */
  std::vector<std::pair<K, V>> rb_in_order(RbNode<K, V>* subtree,
                                           const K lower_bound,
                                           const K upper_bound) const;
  void rb_transplant(RbNode<K, V>* u, RbNode<K, V>* v);
  void rb_left_rotate(RbNode<K, V>* node);
  void rb_right_rotate(RbNode<K, V>* node);
  void rb_insert(RbNode<K, V>* node);
  void rb_insert_fixup(RbNode<K, V>* node);
  void rb_delete(RbNode<K, V>* node);
  void rb_delete_fixup(RbNode<K, V>* node);

public:
  /**
   * @brief Constructs a MemTable object, the maximum size
   * given by the parameter. All inserts beyond that size will
   * throw an error, see Put() method for details.
   *
   * @param memtable_size The size, in elements, of the memtable.
   */
  MemTable(int memtable_size);

  /**
   * @brief Returns a string representation of the tree,
   * meant only for visualization purposes.
   *
   * @return std::string
   */
  std::string Print() const;

  /**
   * @brief Get a value by key. Returns nullptr if does not exist.
   *
   * @param key The key to search for.
   * @return V* A pointer to the value, or nullptr.
   */
  V* Get(const K key) const;

  /**
   * @brief Put a value into the tree. Replaces the value if the key
   * was previously in the tree. If the value was present, return it
   * back out. If not, returns std::nullopt.
   *
   * If the table is full (see MemTable(int) constructor), throws
   * a `MemTableFullException`.
   *
   * @param key The key to insert.
   * @param value The value to insert.
   * @return std::optional<V> The old value, if it existed.
   */
  std::optional<V> Put(const K key, const V value);

  /**
   * @brief Get a vector of sorted (key, value) pairs, where all
   * keys k are such that lower <= k <= upper. The ranges do not
   * have to actually be keys, and do not have to be within the range
   * of actual keys.
   *
   * @param lower The lower bound, inclusive.
   * @param upper THe upper bound, inclusive.
   * @return std::vector<std::pair<K, V>> A list of ordered pairs,
   *    with lowest key earliest.
   */
  std::vector<std::pair<K, V>> Scan(const K lower, const K upper) const;

  /**
   * @brief Deletes a key from the table. Returns a nullptr if the
   * key was never there, or a pointer to the old value if it is.
   *
   * @param key The key to delete
   * @return V* A pointer to the old value, or a nullptr
   */
  V* Delete(const K key);
};
