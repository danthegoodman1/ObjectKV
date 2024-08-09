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

type (
	SegmentReader struct {
		rowIterBlockOffset int

		metadata *segmentMetadata

		// options
		options SegmentReaderOptions
	}

	segmentMetadata struct {
		bloomFilter *bloom.BloomFilter

		firstKey []byte // todo
		lastKey  []byte // todo

		blockIndex any // todo map of (start, (offset, size))
	}
)

func NewSegmentReader(opts SegmentReaderOptions) SegmentReader {
	sr := SegmentReader{
		options: opts,
	}

	return sr
}

// LoadCachedMetadata loads in cached metadata
func (s *SegmentReader) LoadCachedMetadata(metadata *segmentMetadata) {
	panic("todo")
}

// FetchAndLoadMetadata will load the metadata from the file it not already held in the reader, then returns it (for caching).
func (s *SegmentReader) FetchAndLoadMetadata() (*segmentMetadata, error) {
	panic("todo")
}

// probeBloomFilter probes a bloom filter for whether they key might exist within a block in the file.
//
// Instantly returns true if no bloom filter exists.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) probeBloomFilter(key string) (bool, error) {
	panic("todo")
}

var ErrNoMoreRows = errors.New("no more rows")

// RowIter creates a new row iterator. This should only really be used for compaction, as this just starts loading
// blocks and returning rows.
//
// Returns io.EOF when there are no more rows.
//
// Fetches the metadata if not already loaded.
//
// TODO this can be done logically by just reading blocks
func (s *SegmentReader) RowIter() ([]any, error) {
	// todo read block starting at offset
	panic("todo")
}

// readBlockAtOffset will read a data block at an offset, decompress and deserialize it.
//
// Will error if the offset is not a valid block starting point.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) readBlockAtOffset(offset int) (any, error) {
	// todo read the data at the offset, reading the index at the offset
	// todo decompress and deserialize
	// todo return rows
	panic("todo")
}

func (s *SegmentReader) GetRow(key []byte) ([]byte, error) {
	panic("todo")
}

func (s *SegmentReader) GetRange(start, end []byte) ([]byte, error) {
	panic("todo")
}
