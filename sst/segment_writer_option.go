package sst

type SegmentWriterOption func(writer *SegmentWriter)

// WriterLocalCacheDir will concurrently write the segment to the local cache directory
func WriterLocalCacheDir(path string) SegmentWriterOption {
	return func(writer *SegmentWriter) {
		writer.localCacheDir = &path
	}
}
