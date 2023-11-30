#include "kvstore.hpp"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "lsm.hpp"
#include "manifest.hpp"
#include "memtable.hpp"
#include "naming.hpp"
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
  std::unique_ptr<Filter> filter_serializer;
  std::unique_ptr<Sstable> sstable_serializer;
  DbNaming naming;
  MemTable memtable;

  std::optional<Manifest> manifest;
  std::optional<BufPool> buf;

  bool open{false};
  std::vector<std::unique_ptr<LSMLevel>> levels;
  uint8_t tiers;

  std::unique_ptr<LSMRun> create_level0_run() {
    // Register new run
    uint32_t run_idx = this->levels.front()->NextRun();

    // Create the new filter file
    assert(this->manifest.has_value());
    assert(this->buf.has_value());
    std::unique_ptr<LSMRun> run = std::make_unique<LSMRun>(
        this->naming, 0, run_idx, this->tiers, this->memtable.GetCapacity(),
        this->manifest.value(), this->buf.value(), *this->sstable_serializer,
        *this->filter_serializer);

    // Create the new data file
    uint32_t intermediate = run->NextFile();
    std::string data_name = data_file(this->naming, 0, run_idx, intermediate);

    std::unique_ptr<std::vector<std::pair<K, V>>> memtable_contents =
        this->memtable.ScanAll();
    K min = memtable_contents->front().first;
    K max = memtable_contents->back().first;
    this->sstable_serializer->Flush(data_name, *memtable_contents, true);

    std::string filter_name =
        filter_file(this->naming, 0, run_idx, intermediate);
    this->filter_serializer->Create(filter_name, *memtable_contents);

    run->RegisterNewFile(intermediate, min, max);
    return run;
  }

  void recursively_compact() {
    // While each level overflows, register the overflowed, compacted run into
    // the next level.
    // Keep looping until compaction no longer produces a run
    std::size_t L = 0;
    std::optional<std::unique_ptr<LSMRun>> L_run =
        std::make_optional(this->create_level0_run());

    while (L_run.has_value()) {
      // std::cout << "level " << L
      //           << " was full, compacted into a run for next level!" << '\n';

      std::optional<std::reference_wrapper<LSMLevel>> next_level;
      if (L + 1 < this->levels.size()) {
        next_level = *this->levels.at(L + 1);
      }

      L_run = this->levels.at(L)->RegisterNewRun(std::move(L_run.value()),
                                                 next_level);
      L++;

      // If the levels are full, create a new, final level
      // This should stop the looping, the new level won't do compaction
      // when RegisterNewRun is called.
      if (L == this->levels.size() && L_run.has_value()) {
        this->levels.push_back(std::make_unique<LSMLevel>(
            this->naming, this->tiers, L, true, this->memtable.GetCapacity(),
            this->manifest.value(), this->buf.value(),
            *this->sstable_serializer));
      }
    }
  }

  void flush_memtable() {
    assert(this->buf.has_value());
    assert(this->manifest.has_value());

    // Initialize the first level on the first flush
    if (this->levels.size() == 0) {
      auto level = std::make_unique<LSMLevel>(
          this->naming, this->tiers, 0, false, this->memtable.GetCapacity(),
          this->manifest.value(), this->buf.value(), *this->sstable_serializer);
      this->levels.push_back(std::move(level));
    }

    // std::cout << "FLUSHED, NOW COMPACT!" << '\n';

    this->recursively_compact();
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
    }

    std::fstream f(lockfile_name, std::fstream::binary | std::fstream::out |
                                      std::fstream::trunc);
    f.close();
  }

  void unlock_directory() {
    std::string lockfile_name = lock_file(this->naming);
    if (std::filesystem::exists(lockfile_name)) {
      std::filesystem::remove(lockfile_name);
    }
  }

  void init_directory(const std::filesystem::path& parent_dir) const {
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
          this->naming, this->tiers, level, is_final,
          this->memtable.GetCapacity(), this->manifest.value(),
          this->buf.value(), *this->sstable_serializer);
      this->levels.push_back(std::move(lvl));
    };
  }

 public:
  KvStoreImpl() : memtable(0), open(false){};
  ~KvStoreImpl() { this->Close(); };

  void Open(const std::string& name, const Options options) {
    this->open = true;

    // Initialize the page buffer
    this->buf.emplace(BufPoolTuning{
        .initial_elements = options.buffer_pages_initial.value_or(16),
        .max_elements = options.buffer_pages_maximum.value_or(128),
    });

    if (!options.serialization.has_value() ||
        options.serialization.value() == DataFileFormat::kBTree) {
      this->sstable_serializer =
          std::make_unique<SstableBTree>(this->buf.value());
    } else {
      this->sstable_serializer =
          std::make_unique<SstableNaive>(this->buf.value());
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

    // Initialize filter serializer
    this->filter_serializer =
        std::make_unique<Filter>(this->naming, this->buf.value(), 0xbeef);

    // Initialize the manifest file
    this->manifest.emplace(this->naming, this->tiers,
                           *this->sstable_serializer);

    // Initialize the levels
    this->init_levels();
  }

  [[nodiscard]] std::filesystem::path DataDirectory() const {
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
      if (val.has_value()) {
        if (val.value() == kTombstoneValue) {
          return std::nullopt;
        }
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
