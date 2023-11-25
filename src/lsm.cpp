#include "lsm.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "manifest.hpp"
#include "naming.hpp"
#include "sstable.hpp"

class LSMRun::LSMRunImpl {
 private:
  const DbNaming& naming;
  const int level;
  const int run;
  const uint8_t tiers;

  Manifest& manifest;
  BufPool& buf;
  Sstable& sstable_serializer;
  Filter& filter_serializer;

  std::vector<int> files;

 public:
  LSMRunImpl(const DbNaming& naming, int level, int run, uint8_t tiers,
             std::size_t memtable_capacity, Manifest& manifest, BufPool& buf,
             Sstable& sstable_serializer, Filter& filter_serializer)
      : naming(naming),
        level(level),
        run(run),
        tiers(tiers),
        manifest(manifest),
        buf(buf),
        sstable_serializer(sstable_serializer),
        filter_serializer(filter_serializer) {}

  ~LSMRunImpl() = default;

  [[nodiscard]] int NextFile() const { return this->files.size(); }
  void RegisterNewFile(int intermediate) {
    this->files.push_back(intermediate);
  }

  [[nodiscard]] std::optional<V> Get(K key) const {
    // std::cout << "Get from run " << this->run << '\n';

    for (const auto& file : this->files) {
      bool in_range = this->manifest.InRange(this->level, this->run, file, key);
      // std::cout << "In range for file: "
      //           << data_file(this->naming, this->level, this->run, file) <<
      //           "? "
      //           << in_range << '\n';
      if (in_range) {
        auto filter_name =
            filter_file(this->naming, this->level, this->run, file);
        bool in_filter = this->filter_serializer.Has(filter_name, key);
        if (in_filter) {
          std::fstream f(data_file(this->naming, this->level, this->run, file),
                         std::fstream::binary | std::fstream::in);
          std::optional<V> ret = this->sstable_serializer.GetFromFile(f, key);
          if (ret.has_value()) {
            return ret;
          }
        }
      }
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const {
    (void)lower;
    (void)upper;
    std::vector<std::pair<K, V>> l{};
    return l;
  }
};

LSMRun::LSMRun(const DbNaming& naming, int level, int run, uint8_t tiers,
               std::size_t memtable_capacity, Manifest& manifest, BufPool& buf,
               Sstable& sstable_serializer, Filter& filter_serializer)
    : impl(std::make_unique<LSMRunImpl>(
          naming, level, run, tiers, memtable_capacity, manifest, buf,
          sstable_serializer, filter_serializer)) {}
LSMRun::~LSMRun() = default;

[[nodiscard]] std::optional<V> LSMRun::Get(K key) const {
  return this->impl->Get(key);
}
[[nodiscard]] int LSMRun::NextFile() const { return this->impl->NextFile(); }
void LSMRun::RegisterNewFile(int intermediate) {
  return this->impl->RegisterNewFile(intermediate);
}

class LSMLevel::LSMLevelImpl {
 private:
  const uint64_t max_entries;
  const uint32_t level;
  const std::size_t memory_buffer_size;
  const bool is_final;
  const DbNaming& dbname;

  Manifest& manifest;
  BufPool& buf;

  std::vector<std::unique_ptr<LSMRun>> runs;

 public:
  LSMLevelImpl(const DbNaming& dbname, int level, bool is_final,
               std::size_t memory_buffer_size, Manifest& manifest, BufPool& buf)
      : max_entries(pow(2, level) * memory_buffer_size),
        level(level),
        memory_buffer_size(memory_buffer_size),
        is_final(is_final),
        dbname(dbname),
        manifest(manifest),
        buf(buf) {}
  ~LSMLevelImpl() = default;

  [[nodiscard]] uint32_t Level() const { return this->level; }

  [[nodiscard]] std::optional<V> Get(K key) const {
    // std::cout << "Get from level " << this->level << '\n';

    for (auto run = this->runs.rbegin(); run != this->runs.rend(); ++run) {
      std::optional<V> val = (*run)->Get(key);
      if (val.has_value()) {
        return val;
      }
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const {
    (void)lower;
    (void)upper;
    return {};
  }

  [[nodiscard]] int NextRun() { return this->runs.size(); }
  void RegisterNewRun(std::unique_ptr<LSMRun> run) {
    this->runs.push_back(std::move(run));
  }

  // LSMLevel MergeWith(const LSMLevel& level) {
  //   assert(level.Level() == this->Level());

  //   // TODO(kfp): do an actual merging algorithm
  //   return LSMLevel(this->dbname, this->level + 1, this->is_final,
  //                   this->memory_buffer_size, this->buf);
  // };
};

LSMLevel::LSMLevel(const DbNaming& dbname, int level, bool is_final,
                   std::size_t memory_buffer_size, Manifest& manifest,
                   BufPool& buf)
    : impl(std::make_unique<LSMLevelImpl>(dbname, level, is_final,
                                          memory_buffer_size, manifest, buf)) {}
LSMLevel::~LSMLevel() = default;

[[nodiscard]] int LSMLevel::NextRun() const { return this->impl->NextRun(); }
void LSMLevel::RegisterNewRun(std::unique_ptr<LSMRun> run) {
  return this->impl->RegisterNewRun(std::move(run));
}
uint32_t LSMLevel::Level() const { return this->impl->Level(); }
std::optional<V> LSMLevel::Get(K key) const { return this->impl->Get(key); }
std::vector<std::pair<K, V>> LSMLevel::Scan(K lower, K upper) const {
  return this->impl->Scan(lower, upper);
};