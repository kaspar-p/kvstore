# SSTable format

This document describes the format of the SSTables on the disk.

## ptr (8 bytes)

- Most significant 4 bytes are the offset within the file where the block starts.
- Least significant 4 bytes are number of elements within that block. Elements being (key, value) pairs for leaf nodes or (key, ptr) pairs for internal/root nodes

## BTree order

At least 2, at most the # of (key, ptr) pairs that fit into a page.
There are `ORDER-1` keys and `ORDER` ptrs at most, so:

```txt
8 * (ORDER-1 + ORDER) = PAGE_SIZE <=> ORDER = floor(PAGE_SIZE / 16)
```

For `PAGE_SIZE = 4096 (4kb)` then `ORDER=256`.

## Format

First block is always metadata block. Metadata block has structure:

```txt
[ 00 db 00 be ef 00 db 00 ] (magic number, 8 bytes, funny)
[ 00 00 00 00 00 00 00 01 ] (type field, data files are type `01`)
[ uint64_t ]                (# of key value pairs in the file, 8 bytes)
[ root block ptr ]          (8 bytes)
[ uint64_t ]                (minimum key value)
[ uint64_t ]                (maximum key value)
```

with the rest being `00` until the end of the block.

## BTree leaf node format

This includes the root node if there are <= `ORDER` (key, value) pairs in the file.

```txt
[ 00 db 00 11 ] [ uint32_t ]  (4+4 bytes, magic number for leaf node, then 4 bytes garbage)
[ right leaf block ptr ]      (8 bytes)
[ key ] [ value ]             (8 + 8 bytes)
[ key ] [ value ]             (8 + 8 bytes)
...
[ key ] [ value ]             (8 + 8 bytes)
```

The next ptrs are the macro `BLOCK_NULL` if there is no previous/next.

## BTree internal node format

This includes the root node if there are > `ORDER` (key, value) pairs in the file.

```txt
[ 00 db 00 ff ] [ uint32_t ]  (4+4 bytes, magic number for internal node, then # children)
[ child ptr ]                 (8 bytes)
[ key ] [ child ptr ]         (16 bytes)
...
[ key ] [ child ptr ]         (16 bytes)
[ key ] [ child ptr ]         (16 bytes)
```

Note that there is 1 fewer key than there are pointers, since all keys greater than the `ORDER-1`th key go to the `ORDER`th ptr, no matter what. We pack the magic number and number of elements in the remaining 8 bytes.

Each child ptr is `BLOCK_NULL` if there is no leaf node at the end of it.
