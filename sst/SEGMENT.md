# Segment File Format

All numbers are encoded as little endian.

<!-- TOC -->
* [Segment File Format](#segment-file-format)
  * [Top level format](#top-level-format)
  * [Data block format](#data-block-format)
    * [Size limits](#size-limits)
  * [Meta block format](#meta-block-format)
  * [Block index format](#block-index-format)
    * [Simple block index format](#simple-block-index-format)
    * [Partitioned block index format (not implemented)](#partitioned-block-index-format-not-implemented)
  * [Bloom filter block format](#bloom-filter-block-format)
    * [Single bloom filter](#single-bloom-filter)
    * [Partitioned bloom filter format (not implemented)](#partitioned-bloom-filter-format-not-implemented)
  * [Reading a Segment file](#reading-a-segment-file)
    * [Reading data blocks](#reading-data-blocks)
  * [Writing a Segment file](#writing-a-segment-file)
    * [Caching metadata after write](#caching-metadata-after-write)
<!-- TOC -->

## Top level format

The top-level segment format looks like:

```
data block 1
data block 2
...
data block n
meta block
uint64 byte offset where meta block starts
uint64 meta block hash
uint8 segment file version
```
Meta block byte length can be interpolated by: file size - offset - 17, or read as `fileBytes[offset:length-17]`.

The meta block hash is used for the reader to verify that it is reading a valid segment file, and the metadata has not been corrupted

All versions will have the final 17 bytes of offset, hash, version (at least for the first 256 versions).

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
uint16 first key length
first key bytes
uint16 last key length
last key bytes
block index
bloom filter block
uint8 compression format (0 none, 1 zstd, 2 lz4)
```

## Block index format

```
uint8 simple or partitioned block index (not implemented) (0,1)
simple block index/partitioned block index
```

### Simple block index format

```
uint64 number of block index entries
# REPEATED:
    uint16 block first key length
    key bytes
    uint64 block start offset
    uint64 block raw bytes length
    uint64 block compressed bytes length (0 if not compressed)
    uint64 block hash (post compression)
    ...
```

### Partitioned block index format (not implemented)

## Bloom filter block format

```
uint8 whether no bloom filter, bloom filter, or partitioned bloom filter (not implemented) (0,1,2)
uint64 byte length of bloom filter
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

## Writing a Segment file

Writing is done via the `SegmentWriter` which handles block formation, serialization, and more. Segment writers are single-user per-file.

```
w := NewSegmentWriter()
err := w.WriteRow()
// ... write more rows
err := w.Close()
```

Writing rows is expected to be in order, as the writer is optimized for performance and a low memory footprint.

You must always `.Close()` the segment file.

Any errors that are thrown during `WriteRow` or `Close` are NON-RECOVERABLE. This is because stats are collected before a block is fully flushed (e.g. to an S3 writer), so a block cannot be retried via the Segment writer.

The safest option is to just abort the write and throw away the writer.

If you are using a fan-out external writer (e.g. writing to S3 and local cache), ensure that you clean up any files and properly abort S3 writes.

Additionally, if you are retrying to write a segment file by using a new writer, it's greatly advised to use a unique file name for every `SegmentWriter`.

### Caching metadata after write

The `SegmentWriter.Close` method returns the bytes of the metadata block. This can be immediately used with `SegmentReader.BytesToMetadata(SegmentReader{}, metaBlockBytes)` (effectively a static call) to generate metadata struct that can be cached in memory, and subsequently used for future `SegmentReader.LoadCachedMetadata(metadata)` calls.

This allows you to easily read and cache the metadata block while not persisting the segment file to disk to re-read it back in (i.e. only persisting to object storage).