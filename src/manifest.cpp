#include "manifest.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constants.hpp"
#include "fileutil.hpp"
#include "naming.hpp"
#include "sstable.hpp"

class Manifest::ManifestHandleImpl {
 private:
  const DbNaming& naming;
  const uint8_t tiers;
  const Sstable& serializer;
  const std::optional<bool> compaction;

  std::fstream file;
  std::vector<std::vector<FileMetadata>> levels;

  uint64_t total_number_of_files() {
    uint64_t total = 0;
    for (const auto& level : this->levels) {
      total += level.size();
    }

    return total;
  }

  void to_file() {
    assert(this->file.good());
    assert(!this->file.is_open());

    this->file.open(manifest_file(this->naming),
                    std::fstream::binary | std::fstream::in |
                        std::fstream::out | std::fstream::trunc);

    std::vector<uint64_t> page{};
    page.resize(4);
    put_magic_numbers(page, FileType::kManifest);
    page[2] = this->levels.size();
    page[3] = this->total_number_of_files();

    for (std::size_t level = 0; level < this->levels.size(); level++) {
      uint64_t next_level = (static_cast<uint64_t>(level) << 32) |
                            static_cast<uint64_t>(levels.at(level).size());
      page.push_back(next_level);

      for (const auto& f : this->levels.at(level)) {
        auto file_next =
            static_cast<uint64_t>((static_cast<uint64_t>(f.id.run) << 32) |
                                  static_cast<uint64_t>(f.id.intermediate));
        page.push_back(file_next);
        page.push_back(f.minimum);
        page.push_back(f.maximum);
      }
    }

    int remaining = (kPageSize / sizeof(uint64_t)) -
                    (page.size() % (kPageSize / sizeof(uint64_t)));
    for (int i = 0; i < remaining; i++) {
      page.push_back(0);
    }

    this->file.seekp(0);
    this->file.write(reinterpret_cast<char*>(page.data()),
                     page.size() * sizeof(uint64_t));
    this->file.close();
  }

  void from_file() {
    assert(this->file.good());
    assert(!this->file.is_open());

    this->file.open(manifest_file(this->naming), std::fstream::binary |
                                                     std::fstream::in |
                                                     std::fstream::out);

    std::array<uint64_t, kPageSize / sizeof(uint64_t)> first_page;
    this->file.seekg(0);
    this->file.read(reinterpret_cast<char*>(first_page.data()), kPageSize);

    assert(has_magic_numbers(first_page, FileType::kManifest));

    uint64_t total_levels = first_page[2];
    uint64_t total_files = first_page[3];
    this->levels.resize(total_levels);

    std::size_t data_size = (total_files * 3) + total_levels + 4;
    std::vector<uint64_t> data;
    data.resize(data_size);

    this->file.seekg(0);
    this->file.read(reinterpret_cast<char*>(data.data()),
                    data_size * sizeof(uint64_t));
    assert(this->file.good());

    uint64_t level_start = 4;
    for (uint32_t level = 0; level < total_levels; level++) {
      uint32_t level_num = (data.at(level_start) >> 32);
      uint32_t level_files = (data.at(level_start) << 32) >> 32;
      assert(level_num == level);

      for (uint32_t file = 0; file < level_files; file++) {
        uint32_t idx = level_start + (3 * file) + 1;
        uint32_t run = (data.at(idx + 0) >> 32);
        uint32_t intermediate = (data.at(idx + 0) << 32) >> 32;
        K min = data.at(idx + 1);
        K max = data.at(idx + 2);

        if (level >= this->levels.size()) {
          this->levels.resize(level + 1);
        }

        this->levels.at(level).push_back(FileMetadata{
            .id =
                SstableId{
                    .level = level,
                    .run = run,
                    .intermediate = intermediate,
                },
            .minimum = min,
            .maximum = max,
        });
      }
      level_start += (3 * level_files);
    }
    this->file.close();
  }

  void discover_data() {
    assert(std::filesystem::exists(this->naming.dirpath));

    for (auto const& entry :
         std::filesystem::directory_iterator(this->naming.dirpath)) {
      auto name = entry.path().filename();
      if (is_file_type(name, FileType::kData)) {
        uint32_t level = parse_data_file_level(name);
        uint32_t run = parse_data_file_run(name);
        uint32_t intermediate = parse_data_file_intermediate(name);
        if (this->levels.size() <= level) {
          this->levels.resize(level);
        }

        std::string entry_name = entry.path().filename().string();
        this->levels.at(level).push_back(FileMetadata{
            .id =
                SstableId{
                    .level = level,
                    .run = run,
                    .intermediate = intermediate,
                },
            .minimum = this->serializer.GetMinimum(entry_name),
            .maximum = this->serializer.GetMaximum(entry_name),
        });
      }
    }
  }

 public:
  ManifestHandleImpl(const DbNaming& naming, uint8_t tiers,
                     const Sstable& serializer,
                     const std::optional<bool> compaction)
      : naming(naming),
        tiers(tiers),
        serializer(serializer),
        compaction(compaction) {
    bool exists = std::filesystem::exists(manifest_file(naming));
    if (exists) {
      this->from_file();
    } else {
      this->discover_data();
      this->to_file();
    }
  }

  [[nodiscard]] std::vector<std::string> GetPotentialFiles(uint32_t level,
                                                           uint32_t run,
                                                           K key) {
    if (static_cast<std::size_t>(level) >= this->levels.size()) {
      return {};
    }

    std::vector<std::string> matches{};
    for (const auto& file : this->levels.at(level)) {
      if (file.id.run == run && file.minimum <= key && key <= file.maximum) {
        matches.push_back(data_file(this->naming, file.id.level, file.id.run,
                                    file.id.intermediate));
      }
    }

    return matches;
  };

  [[nodiscard]] bool InRange(uint32_t level, uint32_t run,
                             uint32_t intermediate, K key) const {
    if (level >= this->levels.size()) {
      return false;
    }

    bool in_range = std::any_of(
        this->levels.at(level).begin(), this->levels.at(level).end(),
        [&](const auto& file) {
          return file.id.run == run && file.id.intermediate == intermediate &&
                 file.minimum <= key && key <= file.maximum;
        });

    return in_range;
  }

  std::optional<uint32_t> FirstFileInRange(uint32_t level, uint32_t run,
                            K lower, K upper) const {
    for (const auto& file : this->levels.at(level)) {
      if (file.id.run == run && file.minimum <= upper && file.maximum >= lower ) {
        return file.id.intermediate;
      }
    }
    return std::nullopt;
  }

  void RegisterNewFiles(std::vector<FileMetadata> files) {
    for (const auto& file : files) {
      if (file.id.level >= this->levels.size()) {
        this->levels.resize(file.id.level + 1);
      }
      this->levels.at(file.id.level).push_back(file);
    }

    this->to_file();
  }

  void RemoveFiles(std::vector<std::string> files) {
    for (auto& level : this->levels) {
      auto it = std::remove_if(level.begin(), level.end(), [&](auto& f1) {
        return std::any_of(files.begin(), files.end(), [&](std::string& f2) {
          return data_file(this->naming, f1.id.level, f1.id.run,
                           f1.id.intermediate) == f2;
        });
      });

      level.erase(it, level.end());
    }

    this->to_file();
  }

  [[nodiscard]] int NumLevels() const { return this->levels.size(); }

  [[nodiscard]] int NumRuns(uint32_t level) const {
    std::vector<int> unique_runs;
    unique_runs.resize(this->tiers);

    for (const auto& file : this->levels.at(level)) {
      if (unique_runs.at(file.id.run) == 0) {
        unique_runs.at(file.id.run) = 1;
      }
    }

    int count = 0;
    for (const auto& run_bit : unique_runs) {
      count += run_bit;
    }

    return count;
  }

  [[nodiscard]] int NumFiles(uint32_t level, uint32_t run) const {
    int count = 0;
    for (const auto& file : this->levels.at(level)) {
      if (file.id.run == run) {
        count++;
      }
    }

    return count;
  }

  bool CompactionEnabled() {
    if (this->compaction.has_value()) {
      return this->compaction.value();
    }
    return true;
  }
};

Manifest::Manifest(const DbNaming& naming, uint8_t tiers,
                   const Sstable& serializer, std::optional<bool> compaction)
    : impl(std::make_unique<ManifestHandleImpl>(naming, tiers, serializer,
                                                compaction)) {}
Manifest::~Manifest() = default;

[[nodiscard]] std::vector<std::string> Manifest::GetPotentialFiles(
    uint32_t level, uint32_t run, K key) const {
  return this->impl->GetPotentialFiles(level, run, key);
}
[[nodiscard]] bool Manifest::InRange(uint32_t level, uint32_t run,
                                     uint32_t intermediate, K key) const {
  return this->impl->InRange(level, run, intermediate, key);
}

std::optional<uint32_t> Manifest::FirstFileInRange(uint32_t level, uint32_t run,
                                     K lower, K upper) const {
  return this->impl->FirstFileInRange(level, run, lower, upper);
}

void Manifest::RegisterNewFiles(std::vector<FileMetadata> files) {
  return this->impl->RegisterNewFiles(files);
}

void Manifest::RemoveFiles(std::vector<std::string> filenames) {
  return this->impl->RemoveFiles(filenames);
}

int Manifest::NumLevels() const { return this->impl->NumLevels(); };
int Manifest::NumRuns(uint32_t level) const {
  return this->impl->NumRuns(level);
};
int Manifest::NumFiles(uint32_t level, uint32_t run) const {
  return this->impl->NumFiles(level, run);
};
bool Manifest::CompactionEnabled() const {
  return this->impl->CompactionEnabled();
};