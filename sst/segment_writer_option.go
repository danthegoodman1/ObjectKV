package sst

import "github.com/bits-and-blooms/bloom"

type SegmentWriterOptions struct {
	bloomFilter             *bloom.BloomFilter
	dataBlockThresholdBytes int
	localCacheDir           *string
	zstdCompressionLevel    int // if not 0, then use this
	lz4Compression          bool
}

func DefaultSegmentWriterOptions() SegmentWriterOptions {
	return SegmentWriterOptions{
		bloomFilter:             bloom.NewWithEstimates(1_000_000, 0.01),
		dataBlockThresholdBytes: 4096,
		localCacheDir:           nil,
		zstdCompressionLevel:    1,
		lz4Compression:          false,
	}
}
