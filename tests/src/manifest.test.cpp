#include "manifest.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "filter.hpp"
#include "testutil.hpp"

TEST(Manifest, Initialization) {
  auto naming = create_dir("Manifest.Initialization");
  SstableNaive serializer;
  std::filesystem::remove(manifest_file(naming));

  Manifest m(naming, 2, serializer);
  ASSERT_EQ(m.GetPotentialFiles(1, 0, 0), std::vector<std::string>{});
  ASSERT_EQ(std::filesystem::file_size(manifest_file(naming)) % kPageSize, 0);
}

TEST(Manifest, GetFileMemory) {
  auto naming = create_dir("Manifest.GetFileMemory");
  SstableNaive serializer;
  std::filesystem::remove(manifest_file(naming));

  Manifest m(naming, 2, serializer);
  m.RegisterNewFiles({
      FileMetadata{
          .id =
              SstableId{
                  .level = 0,
                  .run = 0,
                  .intermediate = 0,
              },
          .minimum = 1000,
          .maximum = 5000,
      },
      FileMetadata{
          .id =
              SstableId{
                  .level = 0,
                  .run = 0,
                  .intermediate = 1,
              },
          .minimum = 4 * 1000,
          .maximum = 11 * 1000,
      },
      FileMetadata{
          .id =
              SstableId{
                  .level = 0,
                  .run = 0,
                  .intermediate = 2,
              },
          .minimum = 10 * 1000,
          .maximum = 15 * 1000,
      },
  });
  ASSERT_EQ(std::filesystem::file_size(manifest_file(naming)) % kPageSize, 0);
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 1000), std::vector<std::string>({
                                                 data_file(naming, 0, 0, 0),
                                             }));
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 2000), std::vector<std::string>({
                                                 data_file(naming, 0, 0, 0),
                                             }));
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 4500), std::vector<std::string>({
                                                 data_file(naming, 0, 0, 0),
                                                 data_file(naming, 0, 0, 1),
                                             }));
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 10500), std::vector<std::string>({
                                                  data_file(naming, 0, 0, 1),
                                                  data_file(naming, 0, 0, 2),
                                              }));
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 13500), std::vector<std::string>({
                                                  data_file(naming, 0, 0, 2),
                                              }));
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 16000), std::vector<std::string>({}));
  ASSERT_EQ(m.GetPotentialFiles(5, 0, 16000), std::vector<std::string>({}));
}

TEST(Manifest, GetFileRecovery) {
  auto naming = create_dir("Manifest.GetFileRecovery");
  SstableNaive serializer;

  {
    std::filesystem::remove(manifest_file(naming));
    Manifest m(naming, 2, serializer);
    m.RegisterNewFiles({
        FileMetadata{
            .id =
                SstableId{
                    .level = 0,
                    .run = 0,
                    .intermediate = 0,
                },
            .minimum = 1000,
            .maximum = 5000,
        },
        FileMetadata{
            .id =
                SstableId{
                    .level = 0,
                    .run = 0,
                    .intermediate = 1,
                },
            .minimum = 4 * 1000,
            .maximum = 11 * 1000,
        },
        FileMetadata{
            .id =
                SstableId{
                    .level = 0,
                    .run = 0,
                    .intermediate = 2,
                },
            .minimum = 10 * 1000,
            .maximum = 15 * 1000,
        },
    });
  }

  {
    Manifest m(naming, 2, serializer);
    ASSERT_EQ(std::filesystem::file_size(manifest_file(naming)) % kPageSize, 0);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 1000),
              std::vector<std::string>({data_file(naming, 0, 0, 0)}));
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 2000),
              std::vector<std::string>({data_file(naming, 0, 0, 0)}));
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 4500), std::vector<std::string>({
                                                   data_file(naming, 0, 0, 0),
                                                   data_file(naming, 0, 0, 1),
                                               }));
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 10500), std::vector<std::string>({
                                                    data_file(naming, 0, 0, 1),
                                                    data_file(naming, 0, 0, 2),
                                                }));
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 13500),
              std::vector<std::string>({data_file(naming, 0, 0, 2)}));
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 16000), std::vector<std::string>({}));
    ASSERT_EQ(m.GetPotentialFiles(5, 0, 16000), std::vector<std::string>({}));
  }
}

TEST(Manifest, GetFileRecoveryMany) {
  auto naming = create_dir("Manifest.GetFileRecoveryMany");
  SstableNaive serializer;

  {
    std::filesystem::remove(manifest_file(naming));
    Manifest m(naming, 2, serializer);

    std::vector<FileMetadata> files{};
    for (int i = 0; i < 1000; i++) {
      files.push_back(FileMetadata{
          .id = SstableId{.level = 0,
                          .run = 0,
                          .intermediate = static_cast<uint32_t>(i)},
          .minimum = static_cast<K>(0),
          .maximum = static_cast<K>(i)});
    }

    m.RegisterNewFiles(files);
  }

  {
    Manifest m(naming, 2, serializer);
    ASSERT_EQ(std::filesystem::file_size(manifest_file(naming)) % kPageSize, 0);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 0).size(), 1000);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 500).size(), 500);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 999).size(), 1);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 1000).size(), 0);
  }
}

TEST(Manifest, RemoveFileSimple) {
  auto naming = create_dir("Manifest.GetFileRecoveryMany");
  std::filesystem::remove(manifest_file(naming));
  SstableNaive serializer;

  Manifest m(naming, 2, serializer);
  m.RegisterNewFiles({FileMetadata{
      .id =
          SstableId{
              .level = 0,
              .run = 0,
              .intermediate = 0,
          },
      .minimum = 1000,
      .maximum = 5000,
  }});

  m.RemoveFiles({data_file(naming, 0, 0, 0)});
  ASSERT_EQ(m.GetPotentialFiles(0, 0, 0), std::vector<std::string>{});
}

TEST(Manifest, RemoveFileRecovery) {
  auto naming = create_dir("Manifest.RemoveFileRecovery");
  SstableNaive serializer;

  {
    std::filesystem::remove(manifest_file(naming));
    Manifest m(naming, 2, serializer);

    std::vector<FileMetadata> files{};
    std::vector<std::string> filenames{};
    for (int i = 0; i < 1000; i++) {
      files.push_back(FileMetadata{
          .id = SstableId{.level = 0,
                          .run = 0,
                          .intermediate = static_cast<uint32_t>(i)},
          .minimum = static_cast<K>(0),
          .maximum = static_cast<K>(i)});
      filenames.push_back(data_file(naming, 0, 0, static_cast<uint32_t>(i)));
    }

    m.RegisterNewFiles(files);
    m.RemoveFiles(filenames);

    ASSERT_EQ(m.GetPotentialFiles(0, 0, 500).size(), 0);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 0).size(), 0);
  }

  {
    Manifest m(naming, 2, serializer);
    ASSERT_EQ(std::filesystem::file_size(manifest_file(naming)) % kPageSize, 0);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 500).size(), 0);
    ASSERT_EQ(m.GetPotentialFiles(0, 0, 0).size(), 0);
  }
}
