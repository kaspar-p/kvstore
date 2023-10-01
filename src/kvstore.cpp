#include "kvstore.hpp"

#include "memtable.hpp"

char* DatabaseClosedException::what()
{
  return (char*)"Database still closed, please Open() it first!";
}

struct KvStore::KvStoreImpl
{
  std::unique_ptr<MemTable> memtable;
  bool open;
  K least_key;
  K most_key;
  std::string name;

  KvStoreImpl(void)
  {
    constexpr unsigned long long page = 4 * 1024;
    this->memtable = std::make_unique<MemTable>(page);
    this->open = false;
  };

  ~KvStoreImpl() = default;

  void flush() { std::cout << "hello, world!" << std::endl; };

  void Open(const std::string name, Options options)
  {
    this->open = true;
    this->name = name;
  }

  void Close() { this->open = false; }

  std::vector<std::pair<K, V>> Scan(const K lower, const K upper) const
  {
    std::vector<std::pair<K, V>> l {};
    return l;
  }

  V* Get(const K key) const { return nullptr; }

  void Put(const K key, const K value)
  {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    this->least_key = std::min(this->least_key, key);
    this->most_key = std::max(this->least_key, key);

    try {
      this->memtable->Put(key, value);
    } catch (MemTableFullException& e) {
      this->flush();
      this->memtable->Clear();
      this->memtable->Put(key, value);
    }
  }

  void Delete(const K key) { return; };
};

/* Connect the pImpl (pointer-to-implementation) to the actual class */

KvStore::KvStore()
{
  this->pimpl = std::make_unique<KvStoreImpl>();
}

KvStore::~KvStore() = default;

void KvStore::Open(const std::string name, Options options)
{
  return this->pimpl->Open(name, options);
}

void KvStore::Close()
{
  return this->pimpl->Close();
}

std::vector<std::pair<K, V>> KvStore::Scan(const K lower, const K upper) const
{
  return this->pimpl->Scan(lower, upper);
}

V* KvStore::Get(const K key) const
{
  return this->pimpl->Get(key);
}

void KvStore::Put(const K key, const V value)
{
  return this->pimpl->Put(key, value);
}

void KvStore::Delete(const K key)
{
  return this->pimpl->Delete(key);
}
