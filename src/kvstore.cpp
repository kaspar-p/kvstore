#include "kvstore.hpp"

#include "memtable.hpp"

KvStore::KvStore()
{
  constexpr unsigned long long page = 4 * 1024;
  this->memtable = std::make_unique<MemTable>(page);
}

void KvStore::Open(const std::string name)
{
  this->open = true;
  this->name = name;
}

void KvStore::Close()
{
  this->open = false;
}

std::vector<std::pair<K, V>> KvStore::Scan(const K lower, const K upper) const
{
  std::vector<std::pair<K, V>> l;
  return l;
}

V* KvStore::Get(const K key) const
{
  return nullptr;
}

void KvStore::Put(const K key, const V value)
{
  return;
}

void KvStore::Delete(const K key)
{
  return;
}