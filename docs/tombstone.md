# Tombstones

On a `Delete()` call to the database, a key might be on the lowest level of the LSM tree. That is, unless we do a read-before-write, we have no way of knowing if the key actually exists. For this reason, we put a Tombstone marker in the place of a value, and mark that key as _deleted_.

Note that since a tombstone marker is a _value_, and the only valid values are all 64 bit integers, the tombstone marker will be, in hexadecimal:

```txt
00 db 00 de ad 00 db 00
```

which is the decimal integer:

```txt
61643976284887808
```

because it's funny.

All inserts into the database with this value will throw an `OnlyTheDatabaseCanUseFunnyValues` exception.

## Alternatives

RocksDB ([proof](https://piazza.com/class/lm6cxnn0zm5dl/post/190)) uses metadata alongside a value, with a bit and a sequence number. We aren't doing this, but (at least the tombstone bit) wouldn't be that difficult to do.

We still won't.
