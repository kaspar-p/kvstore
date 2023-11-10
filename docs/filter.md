# Bloom Filters

The project uses block-bloom filters. That is, when a key is requested via `Get(key)`,
an "block hash" function is run. This is the index into a large array of bloom filters.
Each bloom filter is exactly the size of a cache line, meaning that subsequent hashes of
the key only result in at most one CPU cache miss.

In the LSM structure, there is one bloom filter per level.

## File structure

These files host the filters for each level of the LSM tree. Each level has a
block-bloom filter to enhance the read performance of the tree.

As discussed, the first page in the file is the metadata block, and has structure:
```
00 db 00 be ef 00 db 00   (8 bytes)
00 00 00 00 00 00 00 02   (8 bytes) 
[ uint64_t ]              (8 bytes, maximum number of elements)
<zero padding>
```

The rest of the pages are full of bloom filters, the final page being
zero-padded.

## Sizes and numbers

Bloom filters cannot expand beyond their original size, which makes them a good choice
for LSM trees, who do no in-place updating/expansion, unlike B-Trees. That configured 
maximum is saved into the file.

The cache line is taken in the code to be 128 bytes, so each bloom filter has 
128*8 = 1024 bits. We use M=5 in the code, but any M is supported. Each bloom filter
has:
```
entries = bits / bits_per_entry = 1024 / 5 = 204
```

That is, the number of bloom blocks that need to be read are is:
```
number of blocks to read = maximum number / entries = maximum number / 204
```

This number will be rounded up to ensure that more bits than are necessary are persisted.

Taking a page size of 4KB, `4096 / 128 = 32` bloom filters fit into a single page. The index
of the page can be calculated when the "block hash" is run, the hash function that maps
a key into an initial bloom filter.
