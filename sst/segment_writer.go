package sst

type SegmentWriter struct {
	currentBlockSize int
	blockStartKey    []byte

	// options
	localCacheDir *string
}

func NewSegmentWriter(opts ...SegmentWriterOption) SegmentWriter {
	sw := SegmentWriter{}
	for _, opt := range opts {
		opt(&sw)
	}

	return sw
}

// Open the remote file for writing, optionally concurrently writing to a local cache.
func (s *SegmentWriter) Open(path string, localPath *string) error {
	// todo check if something is already open, if so error
	panic("todo")
}

// WriteRow writes a given row to the segment. It is expected that rows are written in order.
func (s *SegmentWriter) WriteRow(key, val []byte, compression string) error {
	// todo write the row for the current block
	// todo write the (padded min) multiple of 4k block to the file
	// todo write the metadata to memory for the block start
	// todo store the row in the bloom filter
	panic("todo")
}

// Close finishes writing the segment file by writing the final metadata to the file and closing the writer.
//
// Once this has completed then the segment is considered durably stored.
func (s *SegmentWriter) Close() error {
	// todo write the block index
	// todo write the bloom filter
	// todo record the first and last value of the file
	// todo write the offset for where in the file the metadata starts
	// todo close the remote file
	// todo close the cached file if exists
	panic("todo")
}
