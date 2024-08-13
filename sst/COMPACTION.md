# Compaction

## Range Compaction

Ranges should be split by size at compaction time. The reason is to keep segments small, so that compaction happens quickly (reduce the amount of time lost by a network failure writing to S3).

The external writer will keep track of how many bytes have been written by the `SegmentWriter`, and split when required, all handled automatically by the compaction strategy.

Splitting only occurs during L0->L1 compaction, as there is no L1->L1 compaction with range compaction, since all parts represent complete, contiguous ranges of key-values. Old segments are ignored and immediately cleaned.