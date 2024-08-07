package sst

type SegmentReaderOption func(reader *SegmentReader)

// ReaderCacheInBackground will launch a goroutine that will read the full segment file down to disk in the background
// if it is not already cached.
func ReaderCacheInBackground() SegmentReaderOption {
	return func(reader *SegmentReader) {
		reader.cacheInBackground = true
	}
}

// ReaderLocalCacheDir will use a local file with the matching path if it exists within the
// cacheDirPath.
//
// This will not cause it to be cached if it doesn't exist, see ReaderCacheInBackground.
func ReaderLocalCacheDir(cacheDirPath string) SegmentReaderOption {
	return func(reader *SegmentReader) {
		reader.localCacheDir = &cacheDirPath
	}
}
