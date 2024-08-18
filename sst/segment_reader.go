package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/bits-and-blooms/bloom"
	"github.com/cespare/xxhash/v2"
	"github.com/google/btree"
	"github.com/klauspost/compress/zstd"
	"io"
)

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
		BloomFilter *bloom.BloomFilter

		// ZSTDCompression is the highest priority compression check
		ZSTDCompression bool
		// ZSTDCompression takes priority
		LZ4Compression bool

		FirstKey []byte
		LastKey  []byte

		BlockIndex *btree.BTreeG[BlockStat]
	}
)

var (
	// UnboundStart indicates that the range should go all the way to the first key
	UnboundStart []byte
	// UnboundEnd indicates that the range should go all the way to the last key
	UnboundEnd = []byte{0xff}
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
	metadata.FirstKey = mustReadBytes(metaReader, firstKeyLength)
	lastKeyLength := int(binary.LittleEndian.Uint16(mustReadBytes(metaReader, 2)))
	metadata.LastKey = mustReadBytes(metaReader, lastKeyLength)

	var err error

	// read bloom filter block
	metadata.BloomFilter, err = s.parseBloomFilterBlock(metaReader)
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
	metadata.BlockIndex, err = s.parseBlockIndex(metaReader)
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
func (s *SegmentReader) parseBlockIndex(metaReader *bytes.Reader) (*btree.BTreeG[BlockStat], error) {
	// we only support simple block index now so can skip first byte
	// metaReader.Seek(1, io.SeekCurrent)
	mustReadBytes(metaReader, 1)

	// read the number of data block index entries
	numEntries := int(binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8)))
	if numEntries == 0 {
		return nil, fmt.Errorf("%w: had no data block entries", ErrInvalidMetaBlock)
	}

	t := btree.NewG[BlockStat](2, func(a, b BlockStat) bool {
		return bytes.Compare(a.FirstKey, b.FirstKey) == -1
	})

	for i := 0; i < numEntries; i++ {
		stat := BlockStat{}

		// read first key length
		keyLength := int(binary.LittleEndian.Uint16(mustReadBytes(metaReader, 2)))

		// read all the data
		stat.FirstKey = mustReadBytes(metaReader, keyLength)
		stat.Offset = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.BlockSize = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.OriginalSize = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.CompressedSize = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		stat.Hash = binary.LittleEndian.Uint64(mustReadBytes(metaReader, 8))
		t.ReplaceOrInsert(stat)
	}

	return t, nil
}

// probeBloomFilter probes a bloom filter for whether they key might exist within a block in the file.
//
// Instantly returns true if no bloom filter exists.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) probeBloomFilter(key []byte) (bool, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return false, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	if s.metadata.BloomFilter == nil {
		return false, nil
	}

	return s.metadata.BloomFilter.Test(key), nil
}

// RowIter creates a new row iterator. This should only really be used for compaction and higher-level range reading,
// as this just starts loading blocks and returning rows.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) RowIter() (*RowIter, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	// collect necessary blocks
	var stats []BlockStat
	s.metadata.BlockIndex.Ascend(func(item BlockStat) bool {
		stats = append(stats, item)
		return true
	})

	return &RowIter{
		reader: s.reader,
		s:      s,
	}, nil
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// ReadBlockWithStat will read a data block at an offset, decompress and deserialize it.
//
// Will error if the offset is not a valid block starting point.
//
// Fetches the metadata if not already loaded.
func (s *SegmentReader) ReadBlockWithStat(stat BlockStat) ([]KVPair, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	_, err := s.reader.Seek(int64(stat.Offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Seek: %w", err)
	}

	// read the block into a reader
	rawBlockBytes := make([]byte, stat.BlockSize)
	bytesRead, err := s.reader.Read(rawBlockBytes)
	if err != nil {
		return nil, fmt.Errorf("error in reader.Read: %w", err)
	}
	if bytesRead != int(stat.BlockSize) {
		return nil, fmt.Errorf("%w when reading raw block bytes", ErrUnexpectedBytesRead)
	}

	decompressedBlockBytes := &bytes.Buffer{}
	// if compressed, decompress it
	if s.metadata.ZSTDCompression {
		dec, err := zstd.NewReader(bytes.NewReader(rawBlockBytes[:stat.CompressedSize]))
		if err != nil {
			return nil, fmt.Errorf("error in zstd.NewReader: %w", err)
		}
		defer dec.Close()

		_, err = io.Copy(decompressedBlockBytes, dec)
		if err != nil {
			return nil, fmt.Errorf("error in io.Copy from zstd decoder to byte buffer: %w", err)
		}
	} else if s.metadata.LZ4Compression {
		// todo decompress lz4
	} else {
		decompressedBlockBytes = bytes.NewBuffer(rawBlockBytes)
	}

	// read the rows
	var rows []KVPair
	totalReadBytes := 0
	for totalReadBytes < int(stat.OriginalSize) {
		pair := KVPair{}
		keyLen := binary.LittleEndian.Uint16(mustReadBytes(decompressedBlockBytes, 2))
		totalReadBytes += 2
		valueLen := binary.LittleEndian.Uint32(mustReadBytes(decompressedBlockBytes, 4))
		totalReadBytes += 4
		pair.Key = mustReadBytes(decompressedBlockBytes, int(keyLen))
		totalReadBytes += int(keyLen)
		pair.Value = mustReadBytes(decompressedBlockBytes, int(valueLen))
		totalReadBytes += int(valueLen)

		rows = append(rows, pair)
	}

	return rows, nil
}

var ErrNoRows = errors.New("no rows found")

// GetRow will check whether a row exists within the segment, fetching the metadata as needed
func (s *SegmentReader) GetRow(key []byte) (KVPair, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return KVPair{}, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	// first test the bloom filter if we have it
	if s.metadata.BloomFilter != nil {
		maybeExists, err := s.probeBloomFilter(key)
		if err != nil {
			return KVPair{}, fmt.Errorf("error probing bloom filter: %w", err)
		} else if !maybeExists {
			return KVPair{}, fmt.Errorf("did not find row in bloom filter: %w", ErrNoRows)
		}
	}

	// find the last block first key before this
	var stat *BlockStat
	s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: key}, func(item BlockStat) bool {
		stat = &item
		return false
	})

	if stat == nil {
		return KVPair{}, fmt.Errorf("did not find potential block: %w", ErrNoRows)
	}

	// otherwise we have the block it might be in
	blockRows, err := s.ReadBlockWithStat(*stat)
	if err != nil {
		return KVPair{}, fmt.Errorf("error in readBlockWithFirstKey: %w", err)
	}

	for _, pair := range blockRows {
		if bytes.Equal(pair.Key, key) {
			return pair, nil
		}
	}

	return KVPair{}, fmt.Errorf("did not find row in block: %w", ErrNoRows)
}

// GetRange will get the range of keys [start, end) from the segment.
// todo delete this method - this should be higher level
func (s *SegmentReader) GetRange(start, end []byte) ([]KVPair, error) {
	if s.metadata == nil {
		_, err := s.FetchAndLoadMetadata()
		if err != nil {
			return nil, fmt.Errorf("error in FetchAndLoadMetadata: %w", err)
		}
	}

	isUnboundStart := bytes.Equal(start, UnboundStart)
	isUnboundEnd := bytes.Equal(end, UnboundEnd)

	// find all blocks data could be in
	stats := map[string]BlockStat{} // map for dedupe

	// for the start of the range, we get any block below it
	if isUnboundStart {
		s.metadata.BlockIndex.AscendLessThan(BlockStat{FirstKey: end}, func(item BlockStat) bool {
			stats[string(item.FirstKey)] = item
			return true
		})
	} else {
		s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: start}, func(item BlockStat) bool {
			stats[string(item.FirstKey)] = item
			return bytes.Compare(start, item.FirstKey) <= 0
		})
	}

	// for the end of the range we have to walk down then up until we hit lower and higher edges
	// need the first one below if it exists
	s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: end}, func(item BlockStat) bool {
		stats[string(item.FirstKey)] = item
		return false
	})

	// walk up
	s.metadata.BlockIndex.AscendGreaterOrEqual(BlockStat{FirstKey: end}, func(item BlockStat) bool {
		if !isUnboundEnd && bytes.Compare(end, item.FirstKey) <= 0 {
			// our key is less than the first of this block
			return false
		}
		stats[string(item.FirstKey)] = item
		return true
	})

	var inclRows []KVPair
	// for each block, get everything that is in the range
	for _, stat := range stats {
		blockRows, err := s.ReadBlockWithStat(stat)
		if err != nil {
			return nil, fmt.Errorf("error in ReadBlockWithStat for offset %d: %w", stat.Offset, err)
		}
		for _, row := range blockRows {
			// unbound start works this way too
			if bytes.Compare(start, row.Key) <= 0 {
				if !isUnboundEnd && bytes.Compare(row.Key, end) >= 0 {
					// if the end is greater than or eq to the current row, break
					break
				}
				inclRows = append(inclRows, row)
			}
		}
	}

	return inclRows, nil
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
