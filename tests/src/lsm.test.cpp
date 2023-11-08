#include "lsm.hpp"

#include <gtest/gtest.h>

#include "buf.hpp"

TEST(LSMLevel, Initialize) {
  BufPool buf(BufPoolTuning{
      .initial_elements = 4,
      .max_elements = 16,
  });
  DbNaming naming = DbNaming{
      .dirpath = "/tmp/LSMLevel.GetNonExistantKey",
      .name = "/tmp/LSMLevel.GetNonExistantKey",
  };
  LSMLevel lsm(naming, 1, false, 4 * kMegabyteSize, buf);
}
