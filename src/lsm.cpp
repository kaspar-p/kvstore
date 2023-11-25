#include "lsm.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "buf.hpp"
#include "constants.hpp"
#include "manifest.hpp"
#include "naming.hpp"

class LSMRun::LSMRunImpl {
 private:
  const DbNaming& naming;

  Manifest& manifest;
  BufPool& buf;

  std::vector<std::string> files;

 public:
  LSMRunImpl(const DbNaming& naming, int level, int run, uint8_t tiers,
             std::size_t memtable_capacity, Manifest& manifest, BufPool& buf)
      : naming(naming), manifest(manifest), buf(buf) {}

  ~LSMRunImpl() = default;

  [[nodiscard]] int NextFile() const { return this->files.size(); }
  void RegisterNewFile(std::string& filename) {
    this->files.push_back(filename);
  }

  [[nodiscard]] std::optional<V> Get(K key) const {
    (void)key;
    return std::nullopt;
  }
};

LSMRun::LSMRun(const DbNaming& naming, int level, int run, uint8_t tiers,
               std::size_t memtable_capacity, Manifest& manifest, BufPool& buf)
    : impl(std::make_unique<LSMRunImpl>(naming, level, run, tiers,
                                        memtable_capacity, manifest, buf)) {}
LSMRun::~LSMRun() = default;

[[nodiscard]] std::optional<V> LSMRun::Get(K key) const {
  return this->impl->Get(key);
}
[[nodiscard]] int LSMRun::NextFile() const { return this->impl->NextFile(); }
void LSMRun::RegisterNewFile(std::string& filename) {
  return this->impl->RegisterNewFile(filename);
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
    for (const auto& run : this->runs) {
      std::optional<V> val = run->Get(key);
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