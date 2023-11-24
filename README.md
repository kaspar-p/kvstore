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

And two Sstable implementations, their interface described in ([./src/sstable.hpp](./src/sstable.hpp)):

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

## Extendible hashing

The Buffer Pool implementation uses extendible hashing based on a Trie (citation needed) data structure, also sometimes called a prefix tree. That is, the extendible hashing internally tracks the number of relevant bits, and traverses the data structure to get the right page.

Extendible hashing grows much easier than traditional hashing, since not every element needs to be re-hashed. This allows the database to offer fast `ShrinkPageBufferMaximum` and `GrowPageBufferMaximum` operations, found in the KvStore.

## File sizes

One of the most interesting decisions we made in this project was to cap the size of the files. File sizes affect the implementation of compaction, flushing the Memtable, and getting from each file. LevelDB (citation needed) and RocksDB (citation needed) also do this.

The file sizes are capped to be the exact same (data) size of the memtable, meaning the files will contain the exact same number of keys that the Memtable is initialized to have as a maximum.

This makes flushing the Memtable into the filesystem simple. Memtables and SSTs are 1:1 with each other.

It also makes the compaction process easier. Being able to merge the data from two files no longer requires in-memory buffers into each of the files, since both files are assumed to be small enough to fit in memory.

However, this offers greater complexity for LSM levels and runs. Now, a single run in an LSM level is not a single file, but rather, multiple files.

The number of tiers-per-level is controlled by the `tiers` option in the `Options` struct, one of the parameters to `KvStore::Open()`.

## LSM structure

The structure of the LSM level was highly influenced by two things: using Dostoevsky, and our previously described file sizes specification.

Each LSM