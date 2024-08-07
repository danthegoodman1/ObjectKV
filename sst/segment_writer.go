package sst

type SegmentWriter struct {
}

// Open the remote file for writing, optionally concurrently writing to a local cache
func (s *SegmentWriter) Open(path string) error {
	// todo check if something is already open, if so error
	panic("todo")
}

// WriteBlock writes a data block to the opened file
func (s *SegmentWriter) WriteBlock(data []byte, compression string) error {
	// todo write the (padded min) 4k block to the file
	// todo write the metadata to memory for the block start
	// todo store the row in the bloom filter
	panic("todo")
}

// Close finishes writing the segment file by writing the final metadata to the
// file and closing the writer. Once this has completed then the segment is considered
// durably stored.
func (s *SegmentWriter) Close() error {
	// todo write the block index
	// todo write the bloom filter
	// todo record the first and last value of the file
	// todo write the offset for where in the file the metadata starts
	// todo close the remote file
	// todo close the cached file if exists
	panic("todo")
}
