package sst

import (
	"errors"
	"github.com/bits-and-blooms/bloom"
	"github.com/danthegoodman1/objectkv/syncx"
)

var (
	backgroundCacheLockMap *syncx.Map[string, bool]
)

func init() {
	backgroundCacheLockMap = syncx.NewMapPtr[string, bool]()
}

type SegmentReader struct {
	rowIterBlockOffset int

	bloomFilter *bloom.BloomFilter

	firstKey []byte // todo
	lastKey  []byte // todo

	blockIndex any // todo map of (start, (offset, size))

	// options
	options SegmentReaderOptions
}

func NewSegmentReader(opts SegmentReaderOptions) SegmentReader {
	sr := SegmentReader{
		options: opts,
	}

	return sr
}

// Open will open a segment file for reading. Automatically will read from the locally cached file if it exists at the
// localPath.
//
// Will start reading and load the metadata in. If you already have the metadata, see OpenWithMetadata
func (s *SegmentReader) Open() error {
	// TODO in goroutine grab background cache lock? maybe this can be package local since files are immutable
	panic("todo")
}

// OpenWithMetadata opens the file for reading with cached metadata
func (s *SegmentReader) OpenWithMetadata(metadata any) error {
	panic("todo")
}

// ProbeBloomFilter probes a bloom filter for whether they key might exist within a block in the file.
// Instantly returns true if no bloom filter exists.
func (s *SegmentReader) ProbeBloomFilter(key string) bool {
	panic("todo")
}

var ErrNoMoreRows = errors.New("no more rows")

// RowIter creates a new row iterator. This should only really be used for compaction, as this just starts loading
// blocks and returning rows.
//
// Returns io.EOF when there are no more rows.
//
// TODO this can be done logically by just reading blocks
func (s *SegmentReader) RowIter() ([]any, error) {
	// todo read block starting at offset
	panic("todo")
}

// ReadBlockAtOffset will read a data block at an offset, decompress and deserialize it.
//
// Will error if the offset is not a valid block starting point
func (s *SegmentReader) ReadBlockAtOffset(offset int) (any, error) {
	// todo read the data at the offset, reading the index at the offset
	// todo decompress and deserialize
	// todo return rows
	panic("todo")
}
