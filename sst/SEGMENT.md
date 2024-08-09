# Segment Format

The top-level segment format looks like:

```
data block 1
data block 2
...
data block n
meta block
uint64 byte offset where meta block starts
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
uint16 last key length
last key bytes
```

## Block index format

### Single block index format

```
uint16 block first key value
uint64 block start offset
```

### Partitioned block index format (not implemented)

## Bloom filter block format

```
uint64 size of bloom filter
bloom filter bytes
```

### Single bloom filter

### Partitioned bloom filter format (not implemented)

## Reading a Segment file

Reading a segment file is done via the `SegmentReader`, which is safe to reuse for the same segment file, but is not thread safe.

Much like reading a parquet file, reading a segment file can take multiple io operations:
1. Read the last 8 bytes (uint64) to get the metadata start offset
2. Read the metadata block to find the data block your key may reside in
3. Read the data block

This is expensive, so like many solutions such as ClickHouse and RocksDB, the metadata for a segment file should be loaded into memory on boot.

The way to accomplish this is the following on boot:

```
// on boot
r := NewSegmentReader()
err := r.Open()
metadata, err := r.FetchAndLoadMetadata()
```

Then subsequent uses:

```
r := NewSegmentReader()
r.LoadCachedMetadata(cachedMetadata)
row, err := r.GetRow([]byte("hey"))
```

For convenience all methods that require metadata (e.g. `GetRow`, `GetRange`) will automatically load the metadata into the reader if it does not already exist, in case you intend to do a read without caching the metadata.

### Reading data blocks

Reading a data block can take 1-2 buffer allocations:
1. Raw block buffer
2. Compressed block buffer (if block compressed, we need to load the compressed block and decompress to the raw block buffer for reading)