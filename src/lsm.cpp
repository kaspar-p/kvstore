#include "lsm.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>

#include "buf.hpp"
#include "constants.hpp"
#include "filter.hpp"
#include "manifest.hpp"
#include "minheap.hpp"
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

  void RegisterNewFile(int intermediate, K minimum, K maximum) {
    this->files.push_back(intermediate);
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

class LSMLevel::LSMLevelImpl {
 private:
  const uint64_t max_entries;
  const uint8_t tiers;
  const uint32_t level;
  const std::size_t memtable_capacity;
  const bool is_final;
  const DbNaming& dbname;

  Manifest& manifest;
  BufPool& buf;
  Sstable& sstable_serializer;
  Filter filter_serializer;

  std::vector<std::unique_ptr<LSMRun>> runs;

  std::unique_ptr<LSMRun> compact_runs(
      std::optional<std::reference_wrapper<LSMLevel>> next_level) {
    uint32_t run_in_next_level = 0;
    if (next_level.has_value()) {
      run_in_next_level = next_level.value().get().NextRun();
    }

    std::unique_ptr<LSMRun> new_run = std::make_unique<LSMRun>(
        this->dbname, this->level + 1, run_in_next_level, this->tiers,
        this->memtable_capacity, this->manifest, this->buf,
        this->sstable_serializer, this->filter_serializer);

    std::vector<std::pair<K, V>> buffer;
    buffer.reserve(this->memtable_capacity);

    // Index of current file to read from for each run
    std::vector<int> file_number(this->runs.size(), 1);

    // Contents of current file for each run
    std::vector<std::vector<std::pair<K, V>>> file_contents(this->runs.size());

    // Initialize heap from first key in each run
    std::vector<K> first_keys(this->runs.size(), 0);
    for (std::size_t run = 0; run < this->runs.size(); run++) {
      std::vector<std::pair<K, V>> contents =
          this->runs.at(run)->GetVectorFromFile(0);
      file_contents.at(run) = contents;
      first_keys.at(run) = contents.at(0).first;
    }

    // Index of current position in file for each run
    std::vector<int> file_cursor(this->runs.size(), 0);

    MinHeap minheap(first_keys);

    int min_run;
    std::optional<std::pair<K, int>> min_pair;
    std::optional<std::pair<K, int>> prev_min_pair = std::nullopt;

    while (!minheap.IsEmpty()) {
      while (!minheap.IsEmpty() && buffer.size() < this->memtable_capacity) {
        min_pair = minheap.Extract();
        min_run = min_pair->second;

        // If new key matches previous key, it is out of date, so don't write it
        if (prev_min_pair == std::nullopt ||
            prev_min_pair->first != min_pair->first) {
          std::pair<K, V> min_kv =
              file_contents.at(min_run).at(file_cursor.at(min_run));
          buffer.push_back(min_kv);
        }

        file_cursor.at(min_run)++;
        // If file position has reached the end of the file, try to load the
        // next file from the same run
        if (file_cursor.at(min_run) >= file_contents.at(min_run).size()) {
          file_cursor.at(min_run) = 0;
          file_contents.at(min_run) = this->runs.at(min_run)->GetVectorFromFile(
              file_number.at(min_run));
          file_number.at(min_run)++;
        }

        if (!file_contents.at(min_run).empty()) {
          // insert next key from this file
          std::pair<K, V> next_key_from_file =
              file_contents.at(min_run).at(file_cursor.at(min_run));
          std::optional<std::pair<K, int>> next_pair_to_insert =
              std::make_pair(next_key_from_file.first, min_run);
          minheap.Insert(next_pair_to_insert.value());
        }

        prev_min_pair = min_pair;
      }

      // Flush buffer to file
      if (!buffer.empty()) {
        uint32_t intermediate = new_run->NextFile();

        // Create the data file in the new level
        std::string data_name = data_file(this->dbname, this->level + 1,
                                          run_in_next_level, intermediate);
        this->sstable_serializer.Flush(data_name, buffer, true);

        // Create the corresponding Bloom Filter
        std::string filter_name = filter_file(this->dbname, this->level + 1,
                                              run_in_next_level, intermediate);
        this->filter_serializer.Create(filter_name, buffer);

        new_run->RegisterNewFile(intermediate, buffer.front().first,
                                 buffer.back().first);
        buffer.clear();
      }
    }

    // Remove the data files after the compaction
    for (auto& run : this->runs) {
      run->Delete();
    }
    this->runs.clear();
    return new_run;
  }

 public:
  LSMLevelImpl(const DbNaming& dbname, uint8_t tiers, int level, bool is_final,
               std::size_t memtable_capacity, Manifest& manifest, BufPool& buf,
               Sstable& sstable_serializer)
      : max_entries(pow(2, level) * memtable_capacity),
        tiers(tiers),
        level(level),
        memtable_capacity(memtable_capacity),
        is_final(is_final),
        dbname(dbname),
        manifest(manifest),
        buf(buf),
        sstable_serializer(sstable_serializer),
        filter_serializer(dbname, buf, 0) {}
  ~LSMLevelImpl() = default;

  [[nodiscard]] uint32_t Level() const { return this->level; }

  [[nodiscard]] std::optional<V> Get(K key) const {
    for (auto run = this->runs.rbegin(); run != this->runs.rend(); ++run) {
      std::optional<V> val = (*run)->Get(key);
      if (val.has_value()) {
        return val;
      }
    }

    return std::nullopt;
  }

  [[nodiscard]] std::vector<std::pair<K, V>> Scan(K lower, K upper) const {
    std::vector<std::vector<std::pair<K, V>>> sorted_buffers;
    for (const auto& run : this->runs) {
      sorted_buffers.push_back(run->Scan(lower, upper));
    }

    return minheap_merge(sorted_buffers);
  }

  [[nodiscard]] int NextRun() { return this->runs.size(); }

  std::optional<std::unique_ptr<LSMRun>> RegisterNewRun(
      std::unique_ptr<LSMRun> run,
      std::optional<std::reference_wrapper<LSMLevel>> next_level) {
    this->runs.push_back(std::move(run));

    if (this->runs.size() == this->tiers &&
        this->manifest.CompactionEnabled()) {
      std::unique_ptr<LSMRun> new_run = this->compact_runs(next_level);
      return std::make_optional<std::unique_ptr<LSMRun>>(std::move(new_run));
    }

    return std::nullopt;
  }
};
LSMLevel::LSMLevel(const DbNaming& dbname, uint8_t tiers, int level,
                   bool is_final, std::size_t memtable_capacity,
                   Manifest& manifest, BufPool& buf,
                   Sstable& sstable_serializer)
    : impl(std::make_unique<LSMLevelImpl>(dbname, tiers, level, is_final,
                                          memtable_capacity, manifest, buf,
                                          sstable_serializer)) {}
LSMLevel::~LSMLevel() = default;

[[nodiscard]] int LSMLevel::NextRun() const { return this->impl->NextRun(); }
std::optional<std::unique_ptr<LSMRun>> LSMLevel::RegisterNewRun(
    std::unique_ptr<LSMRun> run,
    std::optional<std::reference_wrapper<LSMLevel>> next_level) {
  return this->impl->RegisterNewRun(std::move(run), next_level);
}
uint32_t LSMLevel::Level() const { return this->impl->Level(); }
std::optional<V> LSMLevel::Get(K key) const { return this->impl->Get(key); }
std::vector<std::pair<K, V>> LSMLevel::Scan(K lower, K upper) const {
  return this->impl->Scan(lower, upper);
};
