package sst

import "github.com/bits-and-blooms/bloom"

type SegmentWriterOptions struct {
	BloomFilter *bloom.BloomFilter

	DataBlockThresholdBytes uint64
	DataBlockSize           uint64
	// if provided, will also write the segment to a local directory. Write will abort if local OR remote fails.
	LocalCacheDir *string

	ZSTDCompressionLevel int // if not 0, then use this

	LZ4Compression bool
}

func DefaultSegmentWriterOptions() SegmentWriterOptions {
	return SegmentWriterOptions{
		BloomFilter:             bloom.NewWithEstimates(100_000, 0.000001), // 351.02KiB estimated, about 1/100k chance of false positive
		DataBlockThresholdBytes: 3584,
		DataBlockSize:           4096,
		LocalCacheDir:           nil,
		ZSTDCompressionLevel:    0,
		LZ4Compression:          false,
	}
}
