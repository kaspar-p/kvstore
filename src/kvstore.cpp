#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "kvstore.hpp"

#include <assert.h>
#include <libc.h>

#include "memtable.hpp"
#include "sstable.hpp"

char* DatabaseClosedException::what()
{
  return (char*)"Database is closed, please Open() it first!";
};

char* FailedToOpenException::what()
{
  return (char*)"Failed to open database directory!";
};

std::vector<std::string> split_string(std::string s, std::string delimiter)
{
  size_t pos = 0;
  std::vector<std::string> tokens;
  while (pos < s.size()) {
    pos = s.find(delimiter);
    tokens.push_back(s.substr(0, pos));
    s.erase(0, pos + delimiter.length());
  }
  tokens.push_back(s);

  return tokens;
}

constexpr static unsigned int key_size = sizeof(K);
constexpr static unsigned int val_size = sizeof(V);
constexpr static unsigned int page_size = 4096;

struct KvStore::KvStoreImpl
{
  // Necessary
  std::unique_ptr<MemTable> memtable;
  bool open;
  std::vector<std::unique_ptr<std::fstream>> levels;
  SstableNaive sstable;

  // Might not be necessary
  K least_key;
  K most_key;
  long blocks;
  std::filesystem::path dir;

  KvStoreImpl(void)
  {
    int max_elems = page_size / (key_size + val_size);
    int other_elems = 4;  // [ magic number, num elements, min key, max key ]
    this->memtable = std::make_unique<MemTable>(max_elems - other_elems);
    this->open = false;
    this->blocks = 0;
  };

  ~KvStoreImpl() = default;

  void flush_memtable()
  {
    std::fstream file(
        this->datafile(this->blocks),
        std::fstream::binary | std::fstream::out | std::fstream::trunc);

    this->sstable.Flush(file, *this->memtable);
    if (!file.good()) {
      perror("Failed to write serialized block!");
      exit(1);
    }

    this->blocks++;
    this->memtable->Clear();
  };

  std::string datafile(const unsigned int block_num) const
  {
    return this->dir / ("__datafile." + std::to_string(block_num));
  }

  void ensure_fs(const Options options)
  {
    bool dir_exists = std::filesystem::is_directory(this->dir);
    if (dir_exists && options.overwrite) {
      std::filesystem::remove_all(this->dir);
    }

    if (!dir_exists || (dir_exists && options.overwrite)) {
      bool created = std::filesystem::create_directory(this->dir);
      if (!created) {
        perror("Failed to create data directory!");
        throw FailedToOpenException();
      };
    }

    unsigned long created_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::ofstream outfile(this->dir / "__HEADER");
    outfile << "name=" << this->dir << "\n"
            << "created=" << created_time << std::endl;
  }

  void Open(const std::string name, const Options options)
  {
    this->open = true;
    this->dir = name;

    this->ensure_fs(options);
  }

  void Close() { this->open = false; }

  std::vector<std::pair<K, V>> Scan(const K lower, const K upper) const
  {
    std::vector<std::pair<K, V>> memtable_l =
        this->memtable->Scan(lower, upper);

    return memtable_l;
  }

  std::optional<V> level_get(const std::size_t level, const K key) const
  {
    assert(level < this->levels.size());
    this->levels.at(level)->open("test.txt");
    // levelf.open("test.txt", std::fstream::in | std::fstream::out);

    return std::nullopt;
  }

  std::optional<V> sst_get(const K key) const
  {
    for (int i = 0; i < this->blocks; i++) {
      // std::cout << "processing block file: " << i << std::endl;

      std::ifstream file {this->datafile(i)};
      std::stringstream buf;
      buf << file.rdbuf();
      std::vector<std::string> pair_strs = split_string(buf.str(), ".");
      for (std::string pair_str : pair_strs) {
        // std::cout << "processing pair str: " + pair_str << std::endl;

        pair_str.erase(0, 1);  // Remove first elem '('
        pair_str.erase(pair_str.size() - 1);  // Remove last elem ')'
        std::vector<std::string> pair = split_string(pair_str, ",");
        assert(pair.size() == 2);
        if (std::stoull(pair[0]) == key) {
          return std::make_optional(std::stoull(pair[1]));
        };
      }
    }

    return std::nullopt;
  }

  std::optional<V> Get(const K key) const
  {
    V* mem_val = this->memtable->Get(key);
    if (mem_val != nullptr) {
      return std::make_optional(*mem_val);
    }

    for (int level = 0; level < this->levels.size(); level++) {
      std::optional<V> val = this->level_get(level, key);
      if (val.has_value()) {
        return val.value();
      }
    }

    return std::nullopt;
  }

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
      this->flush_memtable();
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

std::optional<V> KvStore::Get(const K key) const
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
