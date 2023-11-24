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
#include "dbg.hpp"
#include "lsm.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

const char* OnlyTheDatabaseCanUseFunnyValuesException::what() const noexcept {
  return "Only the database can use funny values! This one is the tombstone "
         "value, and is reserved.";
};

const char* DatabaseInUseException::what() const noexcept {
  return "This data directory is in already in use by an instance of the "
         "database! Cannot have dual-ownership!";
};

const char* DatabaseClosedException::what() const noexcept {
  return "Database is closed, please Open() it first!";
};

const char* FailedToOpenException::what() const noexcept {
  return "Failed to open database directory!";
};

class KvStore::KvStoreImpl {
 private:
  std::unique_ptr<Sstable> sstable_serializer;
  DbNaming naming;
  MemTable memtable;
  bool open;
  std::vector<LSMLevel> levels;
  uint8_t tiers;

  // Might not be necessary
  uint64_t blocks;

  void flush_memtable() {
    std::cout << "FLUSHING!" << '\n';
    std::fstream file(data_file(this->naming, 0, 0, 0), std::fstream::binary |
                                                            std::fstream::in |
                                                            std::fstream::out);
    assert(file.good());
    this->sstable_serializer->Flush(file, this->memtable.ScanAll());
    if (!file.good()) {
      perror("Failed to write serialized block!");
    }

    this->blocks++;
    this->memtable.Clear();
  };

  /**
   * @brief Creates a file in the directory meant for locking the DB. If the
   * file already exists, throws a DatabaseInUseException() to the user.
   */
  void lock_directory() {
    std::string lockfile_name = lock_file(this->naming);
    if (std::filesystem::exists(lockfile_name)) {
      throw DatabaseInUseException();
    } else {
      std::fstream f(lockfile_name, std::fstream::binary | std::fstream::out |
                                        std::fstream::trunc);
      f.close();
    }
  }

  void unlock_directory() {
    assert(this->open);

    std::string lockfile_name = lock_file(this->naming);
    if (std::filesystem::exists(lockfile_name)) {
      std::filesystem::remove(lockfile_name);
    } else {
      throw DatabaseClosedException();
    }
  }

  void init_directory(const std::filesystem::path& parent_dir) {
    if (std::filesystem::exists(parent_dir)) {
      std::filesystem::create_directory(this->naming.dirpath);
    }
  }

 public:
  KvStoreImpl() : open(false), blocks(0), memtable(0){};

  ~KvStoreImpl() = default;

  void Open(const std::string& name, const Options options) {
    this->open = true;

    if (!options.serialization.has_value() ||
        options.serialization.value() == DataFileFormat::BTree) {
      this->sstable_serializer = std::make_unique<SstableBTree>();
    } else {
      this->sstable_serializer = std::make_unique<SstableNaive>();
    }

    this->tiers = options.tiers.value_or(2);
    std::filesystem::path dir = options.dir.value_or("./");
    this->naming = DbNaming{.dirpath = dir / name, .name = name};

    this->init_directory(dir);
    this->lock_directory();

    if (options.buffer_elements.has_value()) {
      this->memtable.IncreaseCapacity(options.buffer_elements.value());
    } else {
      constexpr uint64_t mbData = kMegabyteSize / (kKeySize + kValSize);
      this->memtable.IncreaseCapacity(mbData);
    }
  }

  std::filesystem::path DataDirectory() const {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    return this->naming.dirpath;
  }

  void Close() {
    this->unlock_directory();
    this->open = false;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(const K lower,
                                                  const K upper) const {
    if (!this->open) {
      throw DatabaseClosedException();
    }

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
    if (!this->open) {
      throw DatabaseClosedException();
    }

    // First search the memtable
    V* mem_val = this->memtable.Get(key);

    if (mem_val != nullptr) {
      // If the value is a tombstone, mark it as not present.
      if (*mem_val == kTombstoneValue) {
        return std::nullopt;
      }
      return std::make_optional(*mem_val);
    }

    // Then search through each level, starting at the smallest
    for (const auto& level : this->levels) {
      std::optional<V> val = level.Get(key);
      if (val.has_value() && val.value() != kTombstoneValue) {
        return val;
      }
    }

    return std::nullopt;
  }

  void Put(const K key, const K value) {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    if (value == kTombstoneValue) {
      throw OnlyTheDatabaseCanUseFunnyValuesException();
    }

    try {
      this->memtable.Put(key, value);
    } catch (MemTableFullException& e) {
      this->flush_memtable();
      this->memtable.Clear();
      this->memtable.Put(key, value);
    }
  }

  void Delete(const K key) {
    if (!this->open) {
      throw DatabaseClosedException();
    }

    try {
      // No need to use Delete(), Put replaces the value if it was there.
      this->memtable.Put(key, kTombstoneValue);
    } catch (MemTableFullException& e) {
      this->flush_memtable();
      this->memtable.Clear();
      this->memtable.Put(key, kTombstoneValue);
    }
  };
};

/* Connect the pImpl (pointer-to-implementation) to the actual class */

KvStore::KvStore() { this->impl_ = std::make_unique<KvStoreImpl>(); }

KvStore::~KvStore() = default;

void KvStore::Open(const std::string& name, Options options) {
  return this->impl_->Open(name, options);
}

std::filesystem::path KvStore::DataDirectory() const {
  return this->impl_->DataDirectory();
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
