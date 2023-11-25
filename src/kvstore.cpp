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
#include "manifest.hpp"
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

  std::optional<Manifest> manifest;
  std::optional<BufPool> buf;

  bool open;
  std::vector<std::unique_ptr<LSMLevel>> levels;
  uint8_t tiers;

  void flush_memtable() {
    std::cout << "FLUSHING!" << '\n';
    assert(this->buf.has_value());
    assert(this->manifest.has_value());

    // Initialize the first level on the first flush
    if (this->levels.size() == 0) {
      auto level = std::make_unique<LSMLevel>(
          this->naming, 0, false, this->memtable.GetCapacity(),
          this->manifest.value(), this->buf.value());
      this->levels.push_back(std::move(level));
    }

    int run_idx = this->levels.front()->NextRun();

    std::cout << "NEXT RUN: " << run_idx << '\n';

    std::string filename = data_file(this->naming, 0, run_idx, 0);
    std::fstream file(filename, std::fstream::binary | std::fstream::in |
                                    std::fstream::out | std::fstream::trunc);
    assert(file.good());
    this->sstable_serializer->Flush(file, this->memtable.ScanAll());
    if (!file.good()) {
      perror("Failed to write serialized block!");
      exit(1);
    }

    std::cout << "FILE CREATED!!" << '\n';

    this->memtable.Clear();

    assert(this->manifest.has_value());
    assert(this->buf.has_value());
    std::unique_ptr<LSMRun> run = std::make_unique<LSMRun>(
        this->naming, 0, run_idx, this->tiers, this->memtable.GetCapacity(),
        this->manifest.value(), this->buf.value());

    run->RegisterNewFile(filename);
    this->levels.front()->RegisterNewRun(std::move(run));
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
    std::string lockfile_name = lock_file(this->naming);
    if (std::filesystem::exists(lockfile_name)) {
      std::filesystem::remove(lockfile_name);
    }
  }

  void init_directory(const std::filesystem::path& parent_dir) {
    if (std::filesystem::exists(parent_dir)) {
      std::filesystem::create_directory(this->naming.dirpath);
    }
  }

  void init_levels() {
    assert(this->manifest.has_value());
    assert(this->buf.has_value());

    for (int level = 0; level < this->manifest.value().NumLevels(); level++) {
      bool is_final = level == this->manifest.value().NumLevels() - 1;
      auto lvl = std::make_unique<LSMLevel>(
          this->naming, level, is_final, this->memtable.GetCapacity(),
          this->manifest.value(), this->buf.value());
      this->levels.push_back(std::move(lvl));
    };

    std::cout << "levels is now " << levels.size() << " big" << '\n';
  }

 public:
  KvStoreImpl() : open(false), memtable(0){};
  ~KvStoreImpl() { this->Close(); };

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

    // Initialize the data directory
    this->init_directory(dir);
    this->lock_directory();

    // Initialize the memtable capacity
    std::size_t memtable_capacity = options.memory_buffer_elements.value_or(
        kMegabyteSize / (kKeySize + kValSize));
    this->memtable.IncreaseCapacity(memtable_capacity);

    // Initialize the page buffer
    this->buf.emplace(BufPoolTuning{
        .initial_elements = options.buffer_pages_initial.value_or(16),
        .max_elements = options.buffer_pages_maximum.value_or(128),
    });

    // Initialize the manifest file
    this->manifest.emplace(this->naming, this->tiers,
                           *this->sstable_serializer);

    // Initialize the levels
    this->init_levels();
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
      std::vector<std::pair<K, V>> level_range = level->Scan(lower, upper);
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
      std::optional<V> val = level->Get(key);
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

KvStore::KvStore() { this->impl = std::make_unique<KvStoreImpl>(); }

KvStore::~KvStore() = default;

void KvStore::Open(const std::string& name, Options options) {
  return this->impl->Open(name, options);
}

std::filesystem::path KvStore::DataDirectory() const {
  return this->impl->DataDirectory();
}

void KvStore::Close() { return this->impl->Close(); }

std::vector<std::pair<K, V>> KvStore::Scan(const K lower, const K upper) const {
  return this->impl->Scan(lower, upper);
}

std::optional<V> KvStore::Get(const K key) const {
  return this->impl->Get(key);
}

void KvStore::Put(const K key, const V value) {
  return this->impl->Put(key, value);
}

void KvStore::Delete(const K key) { return this->impl->Delete(key); }
