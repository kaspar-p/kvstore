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
  // Necessary
  std::unique_ptr<MemTable> memtable;
  bool open;
  std::vector<std::unique_ptr<std::fstream>> levels;

  // Might not be necessary
  uint64_t blocks;
  std::filesystem::path dir;

  KvStoreImpl() {
    int max_elems = kPageSize / (kKeySize + kValSize);
    int other_elems = 4;  // [ magic number, num elements, min key, max key ]
    this->memtable = std::make_unique<MemTable>(max_elems - other_elems);
    this->open = false;
    this->blocks = 0;
  };

  ~KvStoreImpl() = default;

  void flush_memtable() {
    std::fstream file(this->datafile(this->blocks), std::fstream::binary |
                                                        std::fstream::out |
                                                        std::fstream::trunc);

    SstableNaive::Flush(file, *this->memtable);
    if (!file.good()) {
      perror("Failed to write serialized block!");
      exit(1);
    }

    this->blocks++;
    this->memtable->Clear();
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
    std::vector<std::pair<K, V>> memtable_l =
        this->memtable->Scan(lower, upper);

    return memtable_l;
  }

  [[nodiscard]] std::optional<V> level_get(const std::size_t level,
                                           const K /*key*/) const {
    assert(level < this->levels.size());
    this->levels.at(level)->open("test.txt");
    // levelf.open("test.txt", std::fstream::in | std::fstream::out);

    return std::nullopt;
  }

  [[nodiscard]] std::optional<V> sst_get(const K key) const {
    for (int i = 0; i < this->blocks; i++) {
      // std::cout << "processing block file: " << i << std::endl;

      std::ifstream file{this->datafile(i)};
      std::stringstream buf;
      buf << file.rdbuf();
      std::vector<std::string> pair_strs =
          split_string(buf.str(), std::string("."));
      for (std::string pair_str : pair_strs) {
        // std::cout << "processing pair str: " + pair_str << std::endl;

        pair_str.erase(0, 1);                 // Remove first elem '('
        pair_str.erase(pair_str.size() - 1);  // Remove last elem ')'
        std::vector<std::string> pair =
            split_string(pair_str, std::string(","));
        assert(pair.size() == 2);
        if (std::stoull(pair[0]) == key) {
          return std::make_optional(std::stoull(pair[1]));
        };
      }
    }

    return std::nullopt;
  }

  [[nodiscard]] std::optional<V> Get(const K key) const {
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

  void Put(const K key, const K value) {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    try {
      this->memtable->Put(key, value);
    } catch (MemTableFullException& e) {
      this->flush_memtable();
      this->memtable->Clear();
      this->memtable->Put(key, value);
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
