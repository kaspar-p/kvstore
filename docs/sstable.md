# SSTable format

## ptr (8 bytes)

- Most significant 4 bytes are the offset within the file where the block starts. 
- Least significant 4 bytes are number of elements within that block. Elements being (key, value) pairs for leaf nodes or (key, ptr) pairs for internal/root nodes

## BTree order

At least 2, at most the # of (key, ptr) pairs that fit into a page.
There are `ORDER-1` keys and `ORDER` ptrs at most, so:
```
8 * (ORDER-1 + ORDER) = PAGE_SIZE <=> ORDER = floor(PAGE_SIZE / 16)
```
For `PAGE_SIZE = 4096 (4kb)` then `ORDER=256`.

## SSTable format

First block is always metadata block. Metadata block has structure:
```
[ 00 db 00 be ef 00 db 00 ] (magic number, 8 bytes, funny)
[ uint64_t ]                (# of key value pairs in the file, 8 bytes)
[ root block ptr ]          (8 bytes)
```
with the rest being `00` until the end of the block.

## BTree leaf node format

This includes the root node if there are <= `ORDER` (key, value) pairs in the file.

```
[ previous leaf block ptr ] (8 bytes)
[ key ] [ value ]           (8 + 8 bytes)
[ key ] [ value ]           (8 + 8 bytes)
...
[ key ] [ value ]           (8 + 8 bytes)
[ next leaf block ptr ]     (8 bytes)
```

The previous/next ptrs are the macro `BLOCK_NULL` if there is no previous/next.

## BTree internal node format

This includes the root node if there are > `ORDER` (key, value) pairs in the file.

```
[ child ptr ] [ key ] (16 bytes)
[ child ptr ] [ key ] (16 bytes)
...
[ child ptr ] [ key ] (16 bytes)
[ child ptr ] [ BLOCK_NULL ] (16 bytes)
```

Note that there is 1 fewer key than there are pointers, since all keys greater than the
`ORDER-1`th key go to the `ORDER`th ptr, no matter what.

Each child ptr is `BLOCK_NULL` if there is no leaf node at the end of it.
