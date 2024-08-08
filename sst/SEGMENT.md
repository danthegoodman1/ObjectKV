# Segment Format

## Data block format

Data blocks have the following format (repeated)

| key length (uint32) | value length (uint32 | key | value |
|---------------------|----------------------|-----|-------|

This formatting occurs before compression.

After a row write to the io.Writer (with optional compression), the size is evaluated to check whether the `dataBlockThresholdBytes` is tripped (default `3584`). This will then cause the data block to be padded with `len(dataBlock) % 4096` zero bytes. This is to reduce the number of excess blocks that are read for a given key. This can be adjusted based on your data, and is per-block, as data writing can exceed the default 4096 `dataBlockSize` typically found on linux file systems.