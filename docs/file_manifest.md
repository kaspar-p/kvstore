# Manifest file

Metadata about the entire LSM tree. A binary file at the top of the directory. Contains the list of files, which files belong to which levels, and the min/max key ranges of each file.

## File format

The first 16 bytes are magic numbers, as in other files. The next are total number of levels and total number of files:

```txt
[ uint64_t ] (file magic)
[ uint64_t ] (type magic)
[ uint64_t ] (num_total_levels)
[ uint64_t ] (num_files_levels)
```

Then, the next 64 bits are

```txt
[ level_num num_files ]
```

where each `level_num` and `num_files` are unsigned 32-bits.

Files are uniquely identified by: their level, their run index, and their file-within-run index. That is, (1, 0, 0) is a file in the second level (0-indexed), the first run, and is the first file within that run.

Since we already know the level_num through parsing the above row, we just store the next two. Each file is 4 bytes for its run index and 4 bytes for its file-within-run index.

Immediately following the file identification (run, file-in-run) integer is two 64-bit integers, corresponding to the minimum and maximum keys within that file, respectively.

For example, if there were 14 files in a level, then after `[ level_num num_files ]`, there would be 14 `uint64_t` integers, the top 32-bits corresponding to the run index, and the bottom 32-bits corresponding to the file-within-run index.

The final page in the file is padded with zeroes.
