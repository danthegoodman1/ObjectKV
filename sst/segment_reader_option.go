package sst

type SegmentReaderOptions struct {
	cacheInBackground bool
	localCacheDir     *string
}

func DefaultSegmentReaderOptions() SegmentReaderOptions {
	return SegmentReaderOptions{
		cacheInBackground: false,
		localCacheDir:     nil,
	}
}
