package sst

import "github.com/bits-and-blooms/bloom"

type SegmentWriterOptions struct {
	bloomFilter *bloom.BloomFilter

	dataBlockThresholdBytes uint64
	dataBlockSize           uint64
	// if provided, will also write the segment to a local directory. Write will abort if local OR remote fails.
	localCacheDir *string

	zstdCompressionLevel int // if not 0, then use this

	lz4Compression bool
}

func DefaultSegmentWriterOptions() SegmentWriterOptions {
	return SegmentWriterOptions{
		bloomFilter:             bloom.NewWithEstimates(100_000, 0.000001), // 351.02KiB estimated, about 1/100k chance of false positive
		dataBlockThresholdBytes: 3584,
		dataBlockSize:           4096,
		localCacheDir:           nil,
		zstdCompressionLevel:    0,
		lz4Compression:          false,
	}
}
