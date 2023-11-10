# Compaction

The intention of compaction is to take N files in a level, and combine them into M files in the next level.

## Size of files

The size of a file heavily affects the compaction algorithm. It's much more difficult to keep running
buffers (in an external sorting way) to existing files than to just _assume_ that every file fits into 
the available memory size.

For this reason, we aim to keep files small, somewhere in the order of 8MB. This is done for two reasons:
  - A file can be entirely skipped if the key range it contains is not relevant to the current sorting.
  - When merging two files, we can just assume they fit entirely into memory and fit them there.

Now this requires that we just have _more_ files per level, but the files have to stay the same size.
This is because if we start with 2MB files, and merge them, then the next level has 4MB files, and the next
has 8MB, and so on. Soon, we won't be able to fit the results of a file into memory.

We want to keep this guarantee, so rather than larger files in each level, we will just have (up to) 
double the amount of files.

## Compaction algorithm

The serializers (`sstable_naive.cpp` and `sstable_btree.cpp`) both implement the `Sstable` interface, which 
has a method `ScanAll` and a method `Flush`:
```cpp
using std::vector, std::string, std::pair, std::unique_ptr;

unique_ptr<vector<pair<K, V>>> ScanAll();
void Flush(string filename, unique_ptr<vector<pair<K, V>>> pairs);
```

Assume the vector fits in memory, and a layout looks like:
```
MemTable: [ ]     (empty)
L1:       [x] [y] (full, requires compaction)
L2:               (empty)
```

The files are named something like `MyDatabase.DATA.L1.R0`, where it means 1) data file, 2) level 1, 3) run 0.
These two files would be called:
```
MyDatabase.DATA.L1.R0
MyDatabase.DATA.L1.R1
```

The process is:
1. Call `ScanAll()` on these two files creating vectors `v1` and `v2`.
2. Perform merge sort on `v1` and `v2` to create a new combined, sorted vector `v3`.
3. Split `v3` into file-size chunks. This can also be done during the merge-sort to
  pipeline the work a little.
4. For each file-size chunk of `v3`, use `Flush` to create a new file in the next level.

If both `v1` and `v2` were completely full (8MB of data worth, say), then two files are created in the next level L2. 
This is one extreme. In the other extreme, all of the keys in `v1` are marked `TOMBSTONE` in `v2`, and only 
one file is persisted into L2 after merging.

Imagine the file `[x]` has keys `1, 3, 4`, and `[y]` has keys `2, 5, 8`. Then we start with:
```
MemTable:               (empty)
L1: [1, 3, 4] [2, 5, 8] (full, requires compaction)
-- no L2 files --
```
and end with:
```
MemTable:                (empty)
L1:                      (empty)
L2: [1, 2, 3] [4, 5, 8]
```
where L2 has double the capacity of L1. This capacity isn't written down anywhere, it's just in code and used
to trigger the compaction of L2 -> L3 _later_ than the compaction of L1 -> L2.

## Skipping files

As mentioned, another benefit of keeping many small files is being able to skip reading through files and being able to
blindly copy, or simply rename.

Each data file has a metadata page as it's very first `kPageSize` bytes, usually 4KB. The metadata page should store the
minimum and maximum keys that the file has. Reading this can allow us to skip entire files.

Imagine a situation like:
```
...
L2: [1,2,3] [4,5,6] | [7,8,9] [10,11,12]
```
where everything to the left of the `|` is the "old data", already sorted, and everything to the right of the `|` is
the data that got flushed out of L1. In this case, we didn't have to do anything to the files, since the min of both
files were above the maximum we had. Here, 7 > 6, so no need to do any sorting. We read a single page out of the file,
and saved lots of compute.
