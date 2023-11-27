# kvstore

A key-value store implementation for CSC443, Fall 2023 by Kaspar Poland, Janine Newton, and Tianji Zhang.

## Building and installing

See the [BUILDING](BUILDING.md) document.

## High-level code architecture

The database (used interchangeable with key-value store) is structured as an Log-Structured Merge (LSM) tree. Other components of the project are:

1. In-memory Memtable ([./src/buf.hpp](./src/memtable.hpp)): a red-black tree to buffer incoming write requests.
1. BufferPool ([./src/buf.hpp](./src/memtable.hpp)): a cache for filesystem pages.
1. Clock Eviction Algorithm ([./src/evict.hpp](./src/evict.hpp)): An eviction algorithm for the Buffer pool cache.
1. Blocked Bloom Filter ([./src/filter.hpp](./src/filter.hpp)): an Bloom Filter to optimize read requests.
1. LSM level manager ([./src/lsm.hpp](./src/lsm.hpp)): Represents a single level in the LSM tree.
1. Manifest file manager ([./src/manifest.hpp](./src/manifest.hpp)): Represents the manifest file and its data.

And two Sstable implementations, their interface described in ([./src/sstable.hpp](./src/sstable.hpp)). The instance variables they are used as are called `sstable_serializer`, but really they do both serialization _and_ deserialization, that is, ser/de.

1. BTree Sstable ser/de ([./src/sstable_btree.cpp]), store the file in BTree format to have log_B search times.
1. Sorted Sstable ser/de ([./src/sstable_naive.cpp]), store the file in a flat sorted way, for binary search.

There are other various utility files that deal with:

1. naming files within the file system ([./src/naming.hpp](./src/naming.hpp)),
1. validating that files have the correct data within them ([./src/fileutil.hpp](./src/fileutil.hpp)),
1. string functions ([./src/dbg.hpp](./src/dbg.hpp)), and
1. a collection of common constants like page size, key size, etc. ([./src/constants.hpp](./src/constants.hpp)).

The entrypoint into the code is the [./src/kvstore.hpp](./src/kvstore.hpp) file, where the main `KvStore` class lives. This is the class that users of the library will instantiate. Like RocksDB (citation needed), the database is only meant to be used as a library, and has no command-line or other interfaces.

The `KvStore` uses `LSMLevel` instances for the LSM levels of the tree, a `ManifestHandle` instance to handle the manifest file, a `BufPool` instance for the buffer pool.

To describe the features of our database, we enumerate some of the design decisions that went into it.

## Blocked Bloom Filters

To improve performance, we used blocked bloom filters over a standard bloom filter. Blocked bloom filters have better cache-locality than standard bloom filters, as they have at most 1 CPU cache miss while fetching. Since bloom filters are frequently accessed, they are often in memory, meaning the saving actually works.

Without a buffer pool, it'd be required to fetch the bloom filter from the filesystem on each read, removing most of the point of the filter itself.

The bloom filters are an interesting implementation. In the [./src/filter.cpp](./src/filter.cpp) file, there is a line:

```cpp
using BloomFilter = std::array<uint8_t, kFilterBytes>
```

That is, the "bloom filter" type is really a custom-implemented bitset with extra bloom logic on top. The original implementation of this class really did use a `std::bitset` to achieve the same goal, but converting these in-memory objects into bytes to be stored on disk (and back again) ate at our performance, so we wrote our own.

The bloom filters are kept as separate files. We talk more about file formats and naming later, but the filter files are named something like:

```txt
some_directory/my-database.FILTER.Lx.Ry.Iz
```

where:

- `some_directory` is the data directory that the user chose to save their data in,
- `my-database` is the unique string name of the database,
- `FILTER` is naturally the type of file to prevent collisions,
- `x` is the _level_ the filter belongs to,
- `y` is the _run_ the filter belongs to, and
- `z` is the _intermediate_ the filter belongs to. The intermediate is just the offset-within-a-run for a file. We talk more about why we chose to have multiple files per level and per run later.

Another interesting bit about the bloom filter is that there is a serializer created per-level. This is to facilitate Monkey, where the bits_per_entry are a function of the level and the number of tiers configured. The serializer will take these numbers into account and instantiate the right number of different hash functions.

They aren't really different, all being xxhash (citation needed) functions with a different starting seed, but it's enough.

## Extendible hashing

The Buffer Pool implementation uses extendible hashing based on a Trie (citation needed) data structure, also sometimes called a prefix tree. That is, the extendible hashing internally tracks the number of relevant bits, and traverses the data structure to get the right page.

The buffer pool implementation works hand-in-hand with the eviction algorithm. They are two different files, and could really be two different libraries. They are split apart for testing, so that extendible hashing growing and shrinking can be tested independently of the eviction algorithm kicking elements out.

The eviction algorithm itself is clock-based, and is generic over its container, just like the buffer pool. That is, it's technically possible to store any structure in the buffer pool, not just pages.

## File sizes

One of the most interesting decisions we made in this project was to cap the size of the files. File sizes affect the implementation of compaction, flushing the Memtable, and getting from each file. LevelDB (citation needed) and RocksDB (citation needed) also do this.

The file sizes are capped to be the exact same (data) size of the memtable, meaning the files will contain the exact same number of keys that the Memtable is initialized to have as a maximum.

This makes flushing the Memtable into the filesystem simple. Memtables and SSTs are 1:1 with each other.

It also makes the compaction process easier. Being able to merge the data from two files no longer requires in-memory buffers into each of the files, since both files are assumed to be small enough to fit in memory.

However, this offers greater complexity for LSM levels and runs. Now, a single run in an LSM level is not a single file, but rather, multiple files.

The number of tiers-per-level is controlled by the `tiers` option in the `Options` struct, one of the parameters to `KvStore::Open()`.

There are some downsides, which we discuss in the next section, LSM structure.

## Database structure

The structure of the LSM level was highly influenced by two things: using Dostoevsky, and our previously described file sizes specification.

The entire LSM tree/database is a single `KvStore` object. 
