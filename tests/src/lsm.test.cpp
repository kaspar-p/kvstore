#include "lsm.hpp"

#include <gtest/gtest.h>

#include "buf.hpp"
#include "testutil.hpp"

TEST(LSMLevel, Initialize) {
  DbNaming naming = create_dir("LSMLevel.GetNonExistantKey");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer;
  Manifest manifest(naming, 4, serializer);

  LSMLevel lsm(naming, 4, 0, false, 20, manifest, buf, serializer);

  ASSERT_EQ(1, 1);
}

TEST(LSMLevel, LevelCorrect) {
  DbNaming naming = create_dir("LSMLevel.LevelCorrect");
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  SstableBTree serializer;
  Manifest manifest(naming, 4, serializer);

  LSMLevel lsm(naming, 4, 1, false, 20, manifest, buf, serializer);

  ASSERT_EQ(lsm.Level(), 1);
}
