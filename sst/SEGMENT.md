# Segment Format

The top-level segment format looks like:

```
data block 1
data block 2
...
data block n
meta block
byte offset where meta block starts (uint64)
```

## Data block format

Data blocks have the following format (bytes, repeated)

```
uint16 key length
uint32 value length
key bytes
value bytes
```

This formatting occurs before compression.

After a row write to the io.Writer (with optional compression), the size is evaluated to check whether the `dataBlockThresholdBytes` is tripped (default `3584`). This will then cause the data block to be padded with `len(dataBlock) % 4096` zero bytes. This is to reduce the number of excess blocks that are read for a given key. This can be adjusted based on your data, and is per-block, as data writing can exceed the default 4096 `dataBlockSize` typically found on linux file systems.

### Size limits

Keys have a size limit of 65,535 (max uint16) bytes, values have a size limit of 4,294,967,295 (max uint32) bytes.

In reality, a developer should implement far lower limits (e.g. max key 512B, max val 16KB).

## Meta block format

```
uint8 single or partitioned block index (not implemented)
block index/partitioned block index
uint8 whether no bloom filter, bloom filter, or partitioned bloom filter (not implemented)
[bloom filter block]
uint8 compression info (none, zstd, lz4)
uint16 first key
uint16 last key
```

## Block index format

### Single block index format


### Partitioned block index format (not implemented)

## Bloom filter block format

```
uint64 size of bloom filter
bloom filter bytes
```

### Single bloom filter

### Partitioned bloom filter format (not implemented)