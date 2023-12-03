#include "lsm_run.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "lsm_level.hpp"
#include "manifest.hpp"
#include "minheap.hpp"
#include "naming.hpp"
#include "sstable.hpp"

struct SstableHandle {
  int intermediate;
  std::unique_ptr<std::fstream> file;
};

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

  std::vector<SstableHandle> files;

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

  void RegisterNewFile(int intermediate, K minimum, K maximum) {
    this->files.push_back(SstableHandle{
        .intermediate = intermediate,
        .file = std::make_unique<std::fstream>(
            std::fstream(data_file(naming, level, run, intermediate)))});
    this->manifest.RegisterNewFiles({FileMetadata{
        .id =
            SstableId{
                .level = static_cast<uint32_t>(this->level),
                .run = static_cast<uint32_t>(this->run),
                .intermediate = static_cast<uint32_t>(intermediate),
            },
        .minimum = minimum,
        .maximum = maximum,
    }});
  }

  [[nodiscard]] std::optional<V> Get(K key) const {
    for (const auto& file : this->files) {
      bool in_range = this->manifest.InRange(this->level, this->run, file, key);
      if (in_range) {
        auto filter_name =
            filter_file(this->naming, this->level, this->run, file);

        bool in_filter = this->filter_serializer.Has(filter_name, key);
        if (in_filter) {
          auto name = data_file(this->naming, this->level, this->run, file);
          std::optional<V> ret =
              this->sstable_serializer.GetFromFile(name, key);
          if (ret.has_value()) {
            return ret;
          }
        }
      }
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const {
    // Find first file that may contain keys within the range
    std::optional<uint32_t> intermediate =
        this->manifest.FirstFileInRange(this->level, this->run, lower, upper);
    if (!intermediate.has_value()) {
      intermediate = 0;
    }

    std::vector<std::pair<K, V>> l{};

    // Now that we have a starting file, we loop until we no longer get matches
    bool matches = true;
    while (matches) {
      std::string sstable =
          data_file(this->naming, this->level, this->run, intermediate.value());
      std::vector<std::pair<K, V>> file_l =
          this->sstable_serializer.ScanInFile(sstable, lower, upper);
      for (auto pair : file_l) {
        l.push_back(pair);
      }

      // If:
      // 1. the last file searched had some keys (they are sorted, so next files
      //    will also not match),
      // 2. there is another file to search, and
      // 3. there are still more keys to find.
      if (file_l.size() > 0 && intermediate.value() + 1 < this->files.size() &&
          file_l.back().first < upper) {
        intermediate.value()++;
      } else {
        matches = false;
      }
    }

    return l;
  }

  void Delete() {
    for (const auto& intermediate : this->files) {
      auto filter =
          filter_file(this->naming, this->level, this->run, intermediate);
      this->filter_serializer.Delete(filter);

      auto data = data_file(this->naming, this->level, this->run, intermediate);
      this->sstable_serializer.Delete(data);

      this->manifest.RemoveFiles(
          {data_file(this->naming, this->level, this->run, intermediate),
           filter_file(this->naming, this->level, this->run, intermediate)});
    }
  }

  std::vector<std::pair<K, V>> GetVectorFromFile(uint32_t file_num) {
    if (file_num >= this->files.size()) {
      return {};
    }

    std::string filename =
        data_file(this->naming, this->level, this->run, file_num);
    return this->sstable_serializer.Drain(filename);
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
std::vector<std::pair<K, V>> LSMRun::Scan(K lower, K upper) const {
  return this->impl->Scan(lower, upper);
}
[[nodiscard]] int LSMRun::NextFile() const { return this->impl->NextFile(); }
void LSMRun::RegisterNewFile(int intermediate, K minimum, K maximum) {
  return this->impl->RegisterNewFile(intermediate, minimum, maximum);
}
void LSMRun::Delete() { return this->impl->Delete(); }
std::vector<std::pair<K, V>> LSMRun::GetVectorFromFile(uint32_t file_num) {
  return this->impl->GetVectorFromFile(file_num);
}
