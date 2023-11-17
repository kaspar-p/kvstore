# Structure of the LSM tree

There are levels, runs within levels, and files within runs. All files have the amount 
of data == the size of the memtable. This ensures we can always pick up a file and read
it into memory, simplifying the compaction algorithm. See [./compaction.md](./compaction.md).

The number of levels grows for the data size, and isn't tunable. It is a result of running
the database.

The number of runs-per-level (`T`) is configurable by the user the database is opened. I haven't
figured out how to solve Open()ing a database with T=4 and then later with T=10.

The number of files in each run is a result of the level. The default for L0 will be 1,
meaning there is a single file per run. Adding new files into L0 == adding new runs into L0.
That is, if the number of files in L0 exceeds T, then compaction is triggered.

The runs-per-level `T` is also the fanout. That is, the rate of data-storage change in
each level. This is represented by changing the number of files going into a run
in a single level. L0 will always have T files, but L1 has T^2 files, and L3 has T^3 files.
A common value for T is 2, or 4, or 10.

## Example

For example, with `T=2`, let a single `M` represent a memtable amount of data, a file. 
Recall that each file is `M` large. Then an LSM tree might look like:
```
MemTable: M
L0: [ M ] [ M ]
L1: [ M M ] [ M M ]
L2: [ M M M M ] [ M M M M ]
L3: [ M M M M M M M M ] [ M M M M M M M M ]
...
```

Recall that if `T=1`, then any compaction is directly sorted in. This can be expensive for
large files. It is equivalent to leveling.

## Compaction Process

For L0, compaction occurs when there are more than T files. In the example above, compaction
occurs on the 5th file.

The compaction is a level-to-level process. That is, L0 compacts into L1, and is finished.
This may trigger another compaction from L1 to L2.


