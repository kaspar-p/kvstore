#include "kvstore.hpp"

#include "memtable.hpp"

char* DatabaseClosedException::what()
{
  return (char*)"Database still closed, please Open() it first!";
}

KvStore::KvStore()
{
  constexpr unsigned long long page = 4 * 1024;
  this->memtable = std::make_unique<MemTable>(page);
  this->open = false;
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
  std::vector<std::pair<K, V>> l {};
  return l;
}

V* KvStore::Get(const K key) const
{
  return nullptr;
}

void KvStore::Put(const K key, const V value)
{
  if (!this->open) {
    throw DatabaseClosedException();
  }

  try {
    this->memtable->Put(key, value);
  } catch (MemTableFullException& e) {
    this->flush();
    this->memtable->Clear();
    this->memtable->Put(key, value);
  }
}

void KvStore::Delete(const K key)
{
  return;
}

void KvStore::TEST_set_memtable_size(unsigned long long capacity)
{
  this->memtable = std::make_unique<MemTable>(capacity);
}

void KvStore::flush()
{
  //
}