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

		metadata *SegmentMetadata

		reader    io.ReadSeeker
		fileBytes int

		// options
		options SegmentReaderOptions
	}

	SegmentMetadata struct {
		bloomFilter *bloom.BloomFilter

		// ZSTDCompression is the highest priority compression check
		ZSTDCompression bool
		// ZSTDCompression takes priority
		LZ4Compression bool

		firstKey []byte
		lastKey  []byte

		blockIndex map[[512]byte]blockStat // todo make ordered map? tree?
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
func (s *SegmentReader) LoadCachedMetadata(metadata *SegmentMetadata) {
	s.metadata = metadata
}

var (
	ErrUnknownSegmentVersion   = errors.New("unknown segment version")
	ErrMismatchedMetaBlockHash = errors.New("mismatched meta block hash")
	ErrInvalidMetaBlock        = errors.New("invalid meta block")
)

// FetchAndLoadMetadata will load the metadata from the file it not already held in the reader, then returns it (for caching).
//
// While a bytes.Reader might be less memory and allocation efficient than inspecting the byte array directly, it is well
// worth it to simplify the code and ensure correctness. This likely only happens once per file anyway with metadata caching.
func (s *SegmentReader) FetchAndLoadMetadata() (*SegmentMetadata, error) {
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

	metadata, err := s.BytesToMetadata(metaBlockBytes)
	if err != nil {
		return nil, fmt.Errorf("error in BytesToMetadata: %w", err)
	}

	s.metadata = metadata
	return metadata, nil
}

// BytesToMetadata turns a metadata byte array into its respective struct.
//
// This is useful if you want to preemptively cache metadata from a recent segment write without providing a reader to
// the entire segment, as the SegmentWriter.Close returns the metadata bytes.
func (s *SegmentReader) BytesToMetadata(metaBlockBytes []byte) (*SegmentMetadata, error) {
	metadata := &SegmentMetadata{}
	metaReader := bytes.NewReader(metaBlockBytes)

	// read the first and last key
	firstKeyLength := int(binary.LittleEndian.Uint16(mustReadBytes(metaReader, 2)))
	metadata.firstKey = mustReadBytes(metaReader, firstKeyLength)
	lastKeyLength := int(binary.LittleEndian.Uint16(mustReadBytes(metaReader, 2)))
	metadata.lastKey = mustReadBytes(metaReader, lastKeyLength)

	var err error

	// read bloom filter block
	metadata.bloomFilter, err = s.parseBloomFilterBlock(metaReader)
	if err != nil {
		return nil, fmt.Errorf("error in parseBloomFilterBlock: %w", err)
	}

	// read compression
	compressionByte := mustReadBytes(metaReader, 1)[0]
	switch compressionByte {
	case 1:
		metadata.ZSTDCompression = true
	case 2:
		metadata.LZ4Compression = true
	}

	// read the block index according to spec
	metadata.blockIndex, err = s.parseBlockIndex(metaReader)
	if err != nil {
		return nil, fmt.Errorf("error in parseBlockIndex: %w", err)
	}

	return metadata, nil
}

func (s *SegmentReader) parseBloomFilterBlock(metaReader *bytes.Reader) (*bloom.BloomFilter, error) {
	enabled := mustReadBytes(metaReader, 1)[0] == 1

	if !enabled {
		return nil, nil
	}

	// read the length of the filter
	bloomLength := binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
	bloomBytes := mustReadBytes(metaReader, int(bloomLength))

	var bloomFilter bloom.BloomFilter
	_, err := bloomFilter.ReadFrom(bytes.NewReader(bloomBytes))
	if err != nil {
		return nil, fmt.Errorf("error in mustReadBytes(metaReader, 8): %w", err)
	}

	return &bloomFilter, nil
}

// parseBlockIndex loads the block index into the SegmentReader's SegmentMetadata using the provided metaReader.
//
// It is assumed that the metaReader is Seeked to the start of the data block index
func (s *SegmentReader) parseBlockIndex(metaReader *bytes.Reader) (map[[512]byte]blockStat, error) {
	// we only support simple block index now so can skip first byte
	// metaReader.Seek(1, io.SeekCurrent)
	mustReadBytes(metaReader, 1)

	// read the number of data block index entries
	numEntries := int(binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8)))
	if numEntries == 0 {
		return nil, fmt.Errorf("%w: had no data block entries", ErrInvalidMetaBlock)
	}

	m := map[[512]byte]blockStat{}

	for i := 0; i < numEntries; i++ {
		stat := blockStat{}

		// read first key length
		keyLength := int(binary.LittleEndian.Uint16(mustReadBytes(metaReader, 2)))

		// read all the data
		stat.firstKey = mustReadBytes(metaReader, keyLength)
		stat.offset = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.rawBytes = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.compressedBytes = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.hash = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		// add to the index
		var key [512]byte
		copy(key[:], stat.firstKey)
		m[key] = stat
	}

	return m, nil
}

// probeBloomFilter probes a bloom filter for whether they key might exist within a block in the file.
//
// Instantly returns true if no bloom filter exists.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) probeBloomFilter(key string) (bool, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return false, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}
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
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	// todo read block starting at offset
	panic("todo")
}

// readBlockAtOffset will read a data block at an offset, decompress and deserialize it.
//
// Will error if the offset is not a valid block starting point.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) readBlockAtOffset(offset int) (any, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	// todo read the data at the offset, reading the index at the offset
	// todo decompress and deserialize
	// todo return rows
	panic("todo")
}

func (s *SegmentReader) GetRow(key []byte) ([]byte, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	panic("todo")
}

func (s *SegmentReader) GetRange(start, end []byte) ([]byte, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	panic("todo")
}

var ErrUnexpectedBytesRead = errors.New("unexpected bytes read")

func readBytes(reader io.Reader, bytes int) ([]byte, error) {
	buf := make([]byte, bytes)
	n, err := reader.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Read: %w", err)
	}
	if n != bytes {
		return nil, fmt.Errorf("%w: expected=%d read=%d", ErrUnexpectedBytesRead, bytes, n)
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
