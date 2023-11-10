# kvstore

A key-value store implementation for CSC443, Fall 2023 by Kaspar Poland, Janine Newton, and Tianji Zhang.

## Building and installing

See the [BUILDING](BUILDING.md) document.

## Architecture + features

- [ ] KvStore Implementation
  - [ ] Get
    - Working in main(), but using text serialization, see below
    - No tests
  - [ ] Put
    - Working in main(), but using text serialization, see below
    - No tests
  - [ ] Scan
  - [ ] Delete
  - [ ] Benchmarks
- [ ] LSM implementations
  - [ ] Levels are compacted into the next level when full.
  - [ ] Get()s start at the top and propagate down.
  - [ ] Tombstones are placed in place of values.
- [x] Blocked Bloom Filter (`Filter`)
  - [x] Creation: happens during the constructor
  - [x] Has
- [ ] Buffer pool
  - [ ] Clock eviction
  - [x] Extendible hashing
  - [x] GetPage
  - [x] PutPage
  - [ ] Generic over contents of page `T`, currently assumes `std::array<std::byte, 4096>`
- [x] Sstable BTree Serialization (BTree file, metadata/internal/leaf pages, search via traversal)
  - [x] Get
  - [x] Flush
  - [x] Scan
- [x] Sstable Naive Serialization (flat file, key/value pages, search via binary-search).
  - [x] Get
  - [x] Flush
  - [x] Scan
- [x] MemTable Implementation
  - [x] Get
  - [x] Put
  - [x] Delete
  - [x] Scan

## TODO

- LSM implementations are not done, the main `KvStore` is not using the `LSMLevel` class yet.
  - There is no compaction algorithm
- Recovery is untested. The starting of a database with existing datafiles is not yet tested.
- We have no experiments/benchmarks for anything!
