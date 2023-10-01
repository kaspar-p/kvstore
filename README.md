# kvstore

A key-value store implementation for CSC443, Fall 2023 by Kaspar Poland, Janine Newton, and Tianji Zhang.

## Building and installing

See the [BUILDING](BUILDING.md) document.

## TODO

- Serialize in a non-text format. All keys/values are each 8 bytes (`uint8_t`), so binary search could be done by jumping through indices. We'd need to solve endian-ness before, that's why it's a text-based serialization right now.
- Read through the data directory if it exists. Currently, the `blocks` counter in KvStoreImpl is always going to start at 0, but it should be set based on the existing data. It WILL overwrite data unless this is fixed!