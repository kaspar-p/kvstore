# File structure

There are three different types of files that get created through running the database. In decreasing order of frequency:
- data files
- filter files
- the manifest file

All files begin with the same 8 byte sequence, to signal they are a part of the DB filesystem.
```
00 db 00 be ef 00 db 00
```

The next 8 bytes are type information:
```
00 00 00 00 00 00 00 <type>
```

Each file has a type to distinguish itself from other files:
```
manifest file = 0x00
data file = 0x01
filter file = 0x02
```

After that point, each file contains other metadata in the metadata block, and the rest of the blocks are also up to the file.

To read more about each structure, see:
- Data files: [sstable.md](./sstable.md)
- Filter files: [filter.md](./filter.md)
