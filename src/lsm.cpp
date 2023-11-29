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

  void Delete() {
    std::cout << "DELETING LEVEL " << this->level << " RUN " << this->run
              << '\n';
    for (const auto& intermediate : this->files) {
      std::filesystem::remove(
          data_file(this->naming, this->level, this->run, intermediate));
      std::filesystem::remove(
          filter_file(this->naming, this->level, this->run, intermediate));
    }
  }

  std::vector<std::pair<K, V>> GetVectorFromFile(uint32_t file_num) {
    std::string filename =
        data_file(this->naming, this->level, this->run, file_num);
    std::fstream f(filename, std::fstream::binary | std::fstream::in);
    if (f.good()) {
      return this->sstable_serializer.Drain(f);
    }
    return {};
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

  std::unique_ptr<LSMRun> compact_runs() {
    std::unique_ptr<LSMRun> new_run = std::make_unique<LSMRun>(
        this->dbname, this->level + 1, 0, this->tiers, this->memtable_capacity,
        this->manifest, this->buf, this->sstable_serializer,
        this->filter_serializer);

    std::vector<std::pair<K, V>> buffer;
    buffer.reserve(this->memtable_capacity);

    // Index of current file to read from for each run
    std::vector<int> file_number(this->runs.size(), 0);

    // Contents of current file for each run
    std::vector<std::vector<std::pair<K, V>>> file_contents(this->runs.size());

    // Initialize heap from first key in each run
    std::vector<K> first_keys(this->runs.size(), 0);
    for (std::size_t run = 0; run < this->runs.size(); run++) {
      std::vector<std::pair<K, V>> contents =
          this->runs.at(run)->GetVectorFromFile(file_number.at(run));
      file_contents.at(run) = contents;
      first_keys.at(run) = contents.at(0).first;
    }

    // Index of current position in file for each run
    std::vector<int> file_cursor(this->runs.size(), 0);
    MinHeap minheap(first_keys);

    std::optional<std::pair<K, int>> min_pair = std::nullopt;
    std::optional<std::pair<K, int>> new_min_pair = minheap.Extract();
    std::optional<std::pair<K, int>> next_pair_to_insert = std::nullopt;
    int min_run;

    while (!minheap.IsEmpty()) {
      while (!minheap.IsEmpty() && buffer.size() < this->memtable_capacity) {
        min_run = new_min_pair->second;

        std::cout << "minheap size: " << minheap.Size()
                  << ", min run: " << min_run
                  << ", file_contents size at min_run: "
                  << file_contents.at(min_run).size()
                  << ", file cursor at min_run: " << file_cursor.at(min_run)
                  << '\n';

        // If new key matches previous key, it is out of date, so don't write it
        if (min_pair == std::nullopt ||
            new_min_pair->first != min_pair->first) {
          std::pair<K, V> min_kv =
              file_contents.at(min_run).at(file_cursor.at(min_run));
          std::cout << "adding " << min_kv.first << " to buffer!" << '\n';
          buffer.push_back(min_kv);
        }

        min_pair = new_min_pair;
        file_cursor.at(min_run)++;

        // If file position has reached the end of the file, try to load the
        // next file from the same run
        if (file_cursor.at(min_run) >= file_contents.at(min_run).size()) {
          file_number.at(min_run)++;
          file_cursor.at(min_run) = 0;
          file_contents.at(min_run) = this->runs.at(min_run)->GetVectorFromFile(
              file_number.at(min_run));
        }

        // Get new smallest key from same file
        if (file_contents.at(min_run).empty()) {
          // no more files in this run, so no new key to insert
          new_min_pair = minheap.Extract();
          std::cout << "extracted: " << new_min_pair->first << ","
                    << new_min_pair->second << '\n';
        } else {
          // insert next key from this file
          std::pair<K, V> next_key_from_file =
              file_contents.at(min_run).at(file_cursor.at(min_run));
          next_pair_to_insert =
              std::make_pair(next_key_from_file.first, min_run);
          new_min_pair = minheap.InsertAndExtract(next_pair_to_insert.value());
          std::cout << "got new min pair: " << new_min_pair.value().first
                    << '\n';
        }
      }

      // Flush buffer to file
      if (!buffer.empty()) {
        int intermediate = new_run->NextFile();
        std::cout << "Flushing to " << data_file(this->dbname, this->level + 1, 0, intermediate) << "!" << '\n';
        std::fstream new_file(
            data_file(this->dbname, this->level + 1, 0, intermediate),
            std::fstream::binary | std::fstream::in | std::fstream::out |
                std::fstream::trunc);
        this->sstable_serializer.Flush(new_file, buffer);
        new_run->RegisterNewFile(intermediate);
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
  std::optional<std::unique_ptr<LSMRun>> RegisterNewRun(
      std::unique_ptr<LSMRun> run) {
    if (this->runs.size() == this->tiers) {
      std::unique_ptr<LSMRun> new_run = this->compact_runs();
      this->runs.push_back(std::move(run));
      std::cout << "l" << this->level
                << " registering new run! runs size: " << runs.size() << '\n';
      return std::make_optional<std::unique_ptr<LSMRun>>(std::move(new_run));
    }
    std::cout << "l" << this->level
              << " registering new run! runs size: " << runs.size() << '\n';
    this->runs.push_back(std::move(run));
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
    std::unique_ptr<LSMRun> run) {
  return this->impl->RegisterNewRun(std::move(run));
}
uint32_t LSMLevel::Level() const { return this->impl->Level(); }
std::optional<V> LSMLevel::Get(K key) const { return this->impl->Get(key); }
std::vector<std::pair<K, V>> LSMLevel::Scan(K lower, K upper) const {
  return this->impl->Scan(lower, upper);
};
