package sst

type SegmentReaderOptions struct {
	// the path for the segment file
	path string
	// whether to cache in the background when reading content
	// todo do we need this? easy for a client to pull it in the background
	cacheInBackground bool
	// check within a local directory for the provided path
	localCacheDir *string
}

func DefaultSegmentReaderOptions() SegmentReaderOptions {
	return SegmentReaderOptions{
		cacheInBackground: false,
		localCacheDir:     nil,
	}
}
