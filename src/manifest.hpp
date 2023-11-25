#pragma once
#include <memory>
#include <string>
#include <vector>

#include "constants.hpp"
#include "naming.hpp"
#include "sstable.hpp"

struct FileMetadata {
  SstableId id;
  K minimum;
  K maximum;
};

class Manifest {
 private:
  class ManifestHandleImpl;
  std::unique_ptr<ManifestHandleImpl> impl;

 public:
  /**
   * @brief Creates an interface for the database's manifest file. If the
   * manifest file does not exist, creates it, but creates it empty. Does not
   * modify a manifest file that already exists in the database directory, just
   * reads it to provide an in-memory cache of the data in that file.
   *
   * @param naming The DB naming scheme.
   */
  Manifest(const DbNaming& naming, uint8_t tiers, const Sstable& serializer);
  ~Manifest();

  /**
   * @brief When new files are created, put that information into the manifest
   * file. Persists this to disk.
   *
   * @param files The files and their metadata to persist into the manifest file
   */
  void RegisterNewFiles(std::vector<FileMetadata> files);

  /**
   * @brief When files are removed via compaction, register that with the
   * manifest file. Persists this to disk.
   *
   * @param filenames The files to remove
   */
  void RemoveFiles(std::vector<std::string> filenames);

  /**
   * @brief For a Get(key) request, search for files in a level if that key is
   * in the files min/max range.
   *
   * @param level The level to search in.
   * @param run The run to search in.
   * @param key The key to search for.
   * @return std::vector<std::string> A list of filenames where key is in the
   * range of the keys in that file.
   */
  [[nodiscard]] std::vector<std::string> GetPotentialFiles(int level, int run,
                                                           K key) const;
  /**
   * @brief Similar to GetPotentialFiles, but the other way around. Asks the
   * manifest file if a key is in a files range. Returns true if the datafile
   * might contain the key.
   *
   * @param level The level of the datafile.
   * @param filename The name of the datafile.
   * @param key The key to search for.
   */
  [[nodiscard]] bool InRange(int level, int run, int intermediate, K key) const;

  [[nodiscard]] int NumLevels() const;
  [[nodiscard]] int NumRuns(int level) const;
  [[nodiscard]] int NumFiles(int level, int run) const;
};
