#include "lsm_run.hpp"
#include "lsm_level.hpp"

#include <gtest/gtest.h>

#include "buf.hpp"
#include "constants.hpp"
#include "testutil.hpp"

TEST(LSMRun, Initialize) {
  DbNaming naming = create_dir("LSMRun.Initialize");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer(buf);
  Filter filter(naming, buf, 0);
  Manifest manifest(naming, 4, serializer, true);

  LSMRun run(naming, 0, 0, 4, 20, manifest, buf, serializer, filter);

  ASSERT_EQ(1, 1);
}

TEST(LSMRun, Scan) {
  DbNaming naming = create_dir("LSMRun.Initialize");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer(buf);
  Filter filter(naming, buf, 0);
  Manifest manifest(naming, 4, serializer, true);

  LSMRun run(naming, 0, 0, 4, 20, manifest, buf, serializer, filter);

  auto f0 = data_file(naming, 0, 0, 0);
  std::vector<std::pair<K, V>> keys = {{1, 2}, {2, 4}, {3, 6}};
  serializer.Flush(f0, keys, true);

  auto f1 = data_file(naming, 0, 0, 1);
  keys = {{4, 8}, {5, 10}, {6, 12}};
  serializer.Flush(f1, keys, true);

  run.RegisterNewFile(0, 1, 3);
  run.RegisterNewFile(1, 4, 6);

  auto v = run.Scan(2, 5);
  ASSERT_EQ(v.size(), 4);
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(v.at(i).first, i + 2);
    ASSERT_EQ(v.at(i).second, 2 * (i + 2));
  }

  // Dangling right edge
  v = run.Scan(3, 20);
  ASSERT_EQ(v.size(), 4);
  ASSERT_EQ(v.at(0).first, 3);
  ASSERT_EQ(v.at(0).second, 6);

  ASSERT_EQ(v.at(1).first, 4);
  ASSERT_EQ(v.at(1).second, 8);

  ASSERT_EQ(v.at(2).first, 5);
  ASSERT_EQ(v.at(2).second, 10);

  ASSERT_EQ(v.at(3).first, 6);
  ASSERT_EQ(v.at(3).second, 12);

  // Dangling left edge
  v = run.Scan(0, 5);
  ASSERT_EQ(v.size(), 5);
  ASSERT_EQ(v.at(0).first, 1);
  ASSERT_EQ(v.at(0).second, 2);

  ASSERT_EQ(v.at(1).first, 2);
  ASSERT_EQ(v.at(1).second, 4);

  ASSERT_EQ(v.at(2).first, 3);
  ASSERT_EQ(v.at(2).second, 6);

  ASSERT_EQ(v.at(3).first, 4);
  ASSERT_EQ(v.at(3).second, 8);

  ASSERT_EQ(v.at(4).first, 5);
  ASSERT_EQ(v.at(4).second, 10);

  // Dangling both edges
  v = run.Scan(0, 20);
  ASSERT_EQ(v.size(), 6);
  for (int i = 0; i < 6; i++) {
    ASSERT_EQ(v.at(i).first, i + 1);
    ASSERT_EQ(v.at(i).second, 2 * (i + 1));
  }
}

TEST(LSMLevel, Initialize) {
  DbNaming naming = create_dir("LSMLevel.GetNonExistantKey");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer(buf);
  Manifest manifest(naming, 4, serializer, true);

  LSMLevel lsm(naming, 4, 0, false, 20, manifest, buf, serializer);

  ASSERT_EQ(1, 1);
}

TEST(LSMLevel, LevelCorrect) {
  DbNaming naming = create_dir("LSMLevel.LevelCorrect");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer(buf);
  Manifest manifest(naming, 4, serializer, true);

  LSMLevel lsm(naming, 4, 1, false, 20, manifest, buf, serializer);

  ASSERT_EQ(lsm.Level(), 1);
}
