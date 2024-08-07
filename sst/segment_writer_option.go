package sst

import "github.com/bits-and-blooms/bloom"

type SegmentWriterOption func(writer *SegmentWriter)

// WriterLocalCacheDir will concurrently write the segment to the local cache directory
func WriterLocalCacheDir(path string) SegmentWriterOption {
	return func(writer *SegmentWriter) {
		writer.localCacheDir = &path
	}
}

// WriterUseZSTDCompression uses ZSTD compression when writing, overwriting LZ4 compression settings.
//
// `level` Must be [1, 11].
func WriterUseZSTDCompression(level int) SegmentWriterOption {
	if level > 11 && level <= 0 {
		globalLogger.Fatal().Msg("WriterUseZSTDCompression level must be [1, 11]")
	}
	return func(writer *SegmentWriter) {
		writer.zstdCompressionLevel = level
	}
}

// WriterUseLZ4Compression uses LZ4 compression. Will be overwritten by WriterUseZSTDCompression level > 0 compression
func WriterUseLZ4Compression() SegmentWriterOption {
	return func(writer *SegmentWriter) {
		writer.lz4Compression = true
	}
}

func WriterUseNewBloomFilter(items uint, falsePositiveRate float64) SegmentWriterOption {
	return func(writer *SegmentWriter) {
		writer.bloomFilter = bloom.NewWithEstimates(items, falsePositiveRate)
	}
}
