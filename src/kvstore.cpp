#include "kvstore.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "lsm.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

const char* DatabaseClosedException::what() const noexcept {
  return "Database is closed, please Open() it first!";
};

const char* FailedToOpenException::what() const noexcept {
  return "Failed to open database directory!";
};

std::vector<std::string> split_string(std::string s, std::string&& delimiter) {
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

struct KvStore::KvStoreImpl {
  MemTable memtable;
  bool open;
  std::vector<LSMLevel> levels;

  // Might not be necessary
  uint64_t blocks;
  std::filesystem::path dir;

  KvStoreImpl() : memtable((kPageSize / (kKeySize + kValSize)) - 4) {
    this->open = false;
    this->blocks = 0;
  };

  ~KvStoreImpl() = default;

  void flush_memtable() {
    std::fstream file(this->datafile(this->blocks), std::fstream::binary |
                                                        std::fstream::out |
                                                        std::fstream::trunc);

    SstableNaive::Flush(file, this->memtable);
    if (!file.good()) {
      perror("Failed to write serialized block!");
    }

    this->blocks++;
    this->memtable.Clear();
  };

  [[nodiscard]] std::string datafile(const unsigned int block_num) const {
    return this->dir / ("__datafile." + std::to_string(block_num));
  }

  void ensure_fs(const Options options) const {
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

    uint64_t created_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::ofstream outfile(this->dir / "__HEADER");
    outfile << "name=" << this->dir << "\n"
            << "created=" << created_time << '\n';
  }

  void Open(const std::string& name, const Options options) {
    this->open = true;
    this->dir = name;

    this->ensure_fs(options);
  }

  void Close() { this->open = false; }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(const K lower,
                                                  const K upper) const {
    // Scan through the memtable
    std::vector<std::pair<K, V>> memtable_range =
        this->memtable.Scan(lower, upper);

    // And each level
    for (const auto& level : this->levels) {
      std::vector<std::pair<K, V>> level_range = level.Scan(lower, upper);
      memtable_range.insert(memtable_range.end(), level_range.begin(),
                            level_range.end());
    }

    return memtable_range;
  }

  [[nodiscard]] std::optional<V> Get(const K key) const {
    // First search the memtable
    V* mem_val = this->memtable.Get(key);
    if (mem_val != nullptr) {
      return std::make_optional(*mem_val);
    }

    // Then search through each level, starting at the smallest
    for (const auto& level : this->levels) {
      std::optional<V> val = level.Get(key);
      if (val.has_value()) {
        return val.value();
      }
    }

    return std::nullopt;
  }

  void Put(const K key, const K value) {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    try {
      this->memtable.Put(key, value);
    } catch (MemTableFullException& e) {
      this->flush_memtable();
      this->memtable.Clear();
      this->memtable.Put(key, value);
    }
  }

  void Delete(const K key) const {
    (void)key;
    std::cout << this->blocks << '\n';
  };
};

/* Connect the pImpl (pointer-to-implementation) to the actual class */

KvStore::KvStore() { this->impl_ = std::make_unique<KvStoreImpl>(); }

KvStore::~KvStore() = default;

void KvStore::Open(const std::string& name, Options options) {
  return this->impl_->Open(name, options);
}

void KvStore::Close() { return this->impl_->Close(); }

std::vector<std::pair<K, V>> KvStore::Scan(const K lower, const K upper) const {
  return this->impl_->Scan(lower, upper);
}

std::optional<V> KvStore::Get(const K key) const {
  return this->impl_->Get(key);
}

void KvStore::Put(const K key, const V value) {
  return this->impl_->Put(key, value);
}

void KvStore::Delete(const K key) { return this->impl_->Delete(key); }
