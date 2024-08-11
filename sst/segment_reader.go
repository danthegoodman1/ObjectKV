package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/bits-and-blooms/bloom"
	"github.com/cespare/xxhash/v2"
	"github.com/danthegoodman1/objectkv/syncx"
	"io"
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

		reader    io.ReadSeeker
		fileBytes int

		// options
		options SegmentReaderOptions
	}

	segmentMetadata struct {
		bloomFilter *bloom.BloomFilter

		firstKey []byte // todo
		lastKey  []byte // todo

		blockIndex any // todo map/array of (start, (blockStat))
	}
)

func NewSegmentReader(reader io.ReadSeeker, fileBytes int, opts SegmentReaderOptions) SegmentReader {
	sr := SegmentReader{
		options:   opts,
		reader:    reader,
		fileBytes: fileBytes,
	}

	return sr
}

// LoadCachedMetadata loads in cached metadata
func (s *SegmentReader) LoadCachedMetadata(metadata *segmentMetadata) {
	s.metadata = metadata
}

var (
	ErrUnknownSegmentVersion   = errors.New("unknown segment version")
	ErrMismatchedMetaBlockHash = errors.New("mismatched meta block hash")
)

// FetchAndLoadMetadata will load the metadata from the file it not already held in the reader, then returns it (for caching).
func (s *SegmentReader) FetchAndLoadMetadata() (*segmentMetadata, error) {
	// get final 17 bytes of file
	_, err := s.reader.Seek(-17, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Seek to last 17 bytes: %w", err)
	}

	// read the bytes
	finalSegmentBytes := make([]byte, 17)
	_, err = s.reader.Read(finalSegmentBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading final segment bytes: %w", err)
	}

	segmentVersion := finalSegmentBytes[16]
	if segmentVersion != 1 {
		return nil, fmt.Errorf("%w: expected=%d got=%d", ErrUnknownSegmentVersion, 1, segmentVersion)
	}

	metaBlockOffset := binary.LittleEndian.Uint64(finalSegmentBytes[0:8])
	metaBlockHash := binary.LittleEndian.Uint64(finalSegmentBytes[8:16])

	// Verify the meta block hash
	_, err = s.reader.Seek(int64(metaBlockOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Seek to meta block offset: %w", err)
	}

	metaBlockBytes := make([]byte, s.fileBytes-int(metaBlockOffset)-17)
	_, err = s.reader.Read(metaBlockBytes)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Read for meta block bytes: %w", err)
	}

	if calculatedHash := xxhash.Sum64(metaBlockBytes); calculatedHash != metaBlockHash {
		return nil, fmt.Errorf("%w: expected=%d got=%d", ErrMismatchedMetaBlockHash, metaBlockHash, calculatedHash)
	}

	// Read the meta block into struct
	s.metadata = &segmentMetadata{}
	metaReader := bytes.NewReader(metaBlockBytes)

	// we only support normal block index now so can skip first byte
	metaReader.Seek(1, io.SeekStart)

	// read the block index according to spec
	err = s.loadBlockIndex(metaReader, int(binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))))
	if err != nil {
		return nil, fmt.Errorf("error in loadBlockIndex: %w", err)
	}

	panic("todo")
}

// loadBlockIndex loads the block index into the SegmentReader's segmentMetadata using the provided metaReader
func (s *SegmentReader) loadBlockIndex(metaReader *bytes.Reader, blockLength int) error {
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

func readBytes(reader io.Reader, bytes int) ([]byte, error) {
	buf := make([]byte, bytes)
	n, err := reader.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Read: %w", err)
	}
	if n != bytes {
		return nil, fmt.Errorf("%w: expected=%d read=%d", ErrUnexpectedBytesWritten, bytes, n)
	}

	return buf, nil
}

func mustReadBytes(reader io.Reader, bytes int) []byte {
	b, err := readBytes(reader, bytes)
	if err != nil {
		panic(err)
	}
	return b
}
