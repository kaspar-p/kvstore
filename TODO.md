# TODO

## Higher

1. Delete() is not working! Likely the LSM compaction is not merging keys in the correct order.
1. LSMLevel does not support Scan(), at all!
    1. LSMRun does, now, but it isn't hooked up.
    1. We also need tests for this. There are no cross-run or cross-level Scan() tests right now.
1. Experiments!
1. Report writing

## Lower

1. Add IncreaseBufferSize() and DecreaseBufferSize() APIs for the buffer pool.
1. Test InRange() for Manifest
1. Implement Dostoevsky, the final level being a single run, not multiple.
1. Monkey implementation for Bloom Filters.
