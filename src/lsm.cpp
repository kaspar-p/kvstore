#include "lsm.hpp"

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>

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

  void Flush(std::fstream& file, std::vector<std::pair<K, V>>& pairs) {
    this->sstable_serializer.Flush(file, pairs);
  }

  std::vector<std::pair<K, V>> GetVectorFromFile(uint32_t file_num) {
    std::string filename = data_file(this->naming, this->level, this->run, file_num);
    std::fstream f(filename, std::fstream::binary | std::fstream::in);
    if (!f.is_open()) {
      std::vector<std::pair<K, V>> vector;
      return vector;
    }
    std::vector<std::pair<K, V>> vector = this->sstable_serializer.ScanInFile(f, 0, 1000); // TODO do we have a ScanAll for this?
    return vector;
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
void LSMRun::Flush(std::fstream& file, std::vector<std::pair<K, V>>& pairs) {
  return this->impl->Flush(file, pairs);
}
std::vector<std::pair<K, V>> LSMRun::GetVectorFromFile(uint32_t file_num) {
  return this->impl->GetVectorFromFile(file_num);
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

  int GetIndexOfSmallestKey(std::vector<K> keys) {
    // TODO: implement min-heap for this
    // TODO: use more recent run if two runs have same key, also need to remove the duplicate
    // TODO: figure out how to remove the duplicate too
    // TODO: also tombstones
    int smallest_index = -1;
    int smallest_key = -1;
    for (int i = 0; i < keys.size(); i++) {
      if (keys[i] != -1 && (smallest_key == -1 || keys[i] <= smallest_key)) {
        smallest_key = keys[i];
        smallest_index = i;
      }
    }
    return smallest_index;
  }

  bool KeysEmpty(const std::vector<K>& keys) {
    for (K key : keys) {
      if (key != -1) {
        return false;
      }
    }
    return true;
  }

  std::unique_ptr<LSMRun> CompactRuns(std::unique_ptr<LSMRun> new_run, uint32_t run, uint32_t level) {
      std::vector<std::pair<K, V>> buffer;
      buffer.reserve(memory_buffer_size);

      // Load first file from each run into current_file_contents_per_run
      // Each contains sorted vector of KV pairs
      std::vector<uint32_t> current_file_num_per_run(this->runs.size(), 0);
      std::vector<std::vector<std::pair<K, V>>> current_file_contents_per_run(this->runs.size());
      std::vector<uint32_t> current_key_index_per_run(this->runs.size(), 0);
      std::vector<K> current_key_per_run(this->runs.size(), 0);

      for (int run_num = 0; run_num < this->runs.size(); run_num++) {
        std::vector<std::pair<K, V>> file_contents = this->runs[run_num]->GetVectorFromFile(current_file_num_per_run[run_num]);
        current_file_contents_per_run[run_num] = file_contents;
        current_key_per_run[run_num] = file_contents[0].first;
      }

      while (!KeysEmpty(current_key_per_run)) {
        // if all vectors are empty, we have merged everything
        while (buffer.size() < memory_buffer_size) {
          // If the end of a file has been reached, get next file
          for (int run_num = 0; run_num < this->runs.size(); run_num++) {
            if (current_key_index_per_run[run_num] >= current_file_contents_per_run[run_num].size()) {
              // load next file from run run_num
              current_file_num_per_run[run_num]++;
              current_key_index_per_run[run_num] = 0;
              current_file_contents_per_run[run_num] = this->runs[run_num]->GetVectorFromFile(current_file_num_per_run[run_num]);
              if (current_file_contents_per_run[run_num].empty()) {
                current_key_per_run[run_num] = -1;
              } else {
                current_key_per_run[run_num] = current_file_contents_per_run[run_num][0].first;
              }
            }
          }

          int smallest_run_num = this->GetIndexOfSmallestKey(current_key_per_run);
          if (smallest_run_num == -1) {
            // no keys left
            break;
          }
          buffer.push_back(current_file_contents_per_run[smallest_run_num][current_key_index_per_run[smallest_run_num]]);
          current_key_index_per_run[smallest_run_num]++;
          // TODO: make this an sstable instead of a sorted vector?
        }

        // If buffer is full, flush to file in new run
        if (buffer.size() == memory_buffer_size) {
          uint32_t intermediate = new_run->NextFile();
          std::fstream new_file(data_file(this->dbname, level, run, intermediate),
                               std::fstream::binary | std::fstream::in |
                                   std::fstream::out | std::fstream::trunc);
          new_run->Flush(new_file, buffer);
          new_run->RegisterNewFile(intermediate);

        buffer.clear();
        }
      }
      return new_run;
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
std::unique_ptr<LSMRun> LSMLevel::CompactRuns(std::unique_ptr<LSMRun> new_run, uint32_t run, uint32_t level) {
  return this->impl->CompactRuns(std::move(new_run), run, level);
}