package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"io"
	"math"
)

type (
	SegmentWriter struct {
		currentRawBlockSize  uint64
		currentBlockStartKey []byte
		blockBuffer          *bytes.Buffer // the buffer for the (un)compressed block
		blockWriter          io.Writer     // write to the blockBuffer with optional compression

		// writes to actual destination (S3 &/ file)
		externalWriter io.Writer

		currentByteOffset uint64      // where we are in the file currently, used for block index
		blockIndex        []blockStat // todo, either a tree or https://github.com/wk8/go-ordered-map
		lastKey           []byte

		options SegmentWriterOptions

		closed bool
	}
)

// NewSegmentWriter creates a new segment writer and opens the file(s) for writing.
//
// A segment writer can never be reused, and is not thread safe.
func NewSegmentWriter(writer io.Writer, opts SegmentWriterOptions) SegmentWriter {
	sw := SegmentWriter{
		options:        opts,
		externalWriter: writer,
		blockIndex:     []blockStat{},
	}

	return sw
}

var (
	ErrWriterClosed           = errors.New("segment writer already closed")
	ErrUnexpectedBytesWritten = errors.New("unexpected number of bytes written")
	ErrKeyTooLarge            = errors.New("key too large, must be <= max uint16 bytes")
	ErrValueTooLarge          = errors.New("value too large, must be <= max uin32 bytes")
	ErrNoRows                 = errors.New("no rows were written, can't have an empty segment file")
)

// WriteRow writes a given row to the segment. Cannot write after the writer is closed.
//
// It is expected that rows are written in order.
func (s *SegmentWriter) WriteRow(key, val []byte) error {
	if len(key) > math.MaxUint16 {
		return fmt.Errorf("%w, got length %d", ErrKeyTooLarge, len(key))
	}
	if len(val) > math.MaxUint32 {
		return fmt.Errorf("%w, got length %d", ErrValueTooLarge, len(val))
	}
	if s.closed {
		return ErrWriterClosed
	}
	useZSTD := s.options.ZSTDCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.LZ4Compression
	if s.blockWriter == nil {
		// Ensure we are at a base state
		s.currentBlockStartKey = key
		s.currentRawBlockSize = 0
		s.blockBuffer = &bytes.Buffer{}

		// create the writer if it doesn't exist, using the correct writer based on compression
		// todo check lz4 compression
		if useZSTD {
			enc, err := zstd.NewWriter(s.blockBuffer, zstd.WithEncoderLevel(zstd.EncoderLevel(s.options.ZSTDCompressionLevel)))
			if err != nil {
				return fmt.Errorf("error in zstd.NewWriter: %w", err)
			}
			s.blockWriter = enc
		} else {
			s.blockWriter = s.blockBuffer // just use the external writer directly
		}
	}

	// update the key tracking for final write
	s.lastKey = key

	// write the row for the current block into the buffer
	rowBuf := make([]byte, 6+len(key)+len(val))
	binary.LittleEndian.PutUint16(rowBuf[0:2], uint16(len(key)))
	binary.LittleEndian.PutUint32(rowBuf[2:6], uint32(len(val)))
	copy(rowBuf[6:], key)
	copy(rowBuf[6+len(key):], val)

	_, err := s.blockWriter.Write(rowBuf)
	if err != nil {
		return fmt.Errorf("error in s.blockWriter.Write (zstd=%t, lz4=%t): %w", useZSTD, useLZ4, err)
	}
	s.currentRawBlockSize += uint64(len(rowBuf))

	if s.options.BloomFilter != nil {
		// store the row in the bloom filter if needed
		s.options.BloomFilter.Add(key)
	}

	if uint64(s.blockBuffer.Len()) >= s.options.DataBlockThresholdBytes {
		err = s.flushCurrentDataBlock()
		if err != nil {
			return fmt.Errorf("error in flushCurrentDataBlock: %w", err)
		}
	}

	return nil
}

func (s *SegmentWriter) flushCurrentDataBlock() error {
	useZSTD := s.options.ZSTDCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.LZ4Compression

	if zstdEncoder, ok := s.blockWriter.(*zstd.Encoder); ok {
		err := zstdEncoder.Close()
		if err != nil {
			return fmt.Errorf("error in zstdEncode.Close(): %w", err)
		}
	}

	// write the metadata to memory for the block start with offset and first key
	stat := blockStat{
		offset:   s.currentByteOffset,
		rawBytes: s.currentRawBlockSize,
		firstKey: s.currentBlockStartKey,
	}
	if useZSTD || useLZ4 {
		stat.compressedBytes = uint64(s.blockBuffer.Len())
	}

	if remainder := s.options.DataBlockSize - uint64(s.blockBuffer.Len())%s.options.DataBlockSize; remainder > 0 {
		// write the (padded min) multiple of 4k block to the file after compression
		bytesWritten, err := s.blockBuffer.Write(make([]byte, remainder))
		if err != nil {
			return fmt.Errorf("error writing padding to externalWriter: %w", err)
		}
		if uint64(bytesWritten) != remainder {
			return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, remainder, bytesWritten)
		}
	}

	stat.finalBytes = uint64(s.blockBuffer.Len())

	blockBytes := s.blockBuffer.Bytes()

	// capture a blockHash of the final block bytes
	blockHash := xxhash.Sum64(blockBytes)
	stat.hash = blockHash

	s.blockIndex = append(s.blockIndex, stat)

	// flush the block buffer
	bytesWritten, err := s.externalWriter.Write(blockBytes)
	if err != nil {
		return fmt.Errorf("error writing raw block writer bytes to external writer: %w", err)
	}
	if bytesWritten != s.blockBuffer.Len() {
		return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, s.blockBuffer.Len(), bytesWritten)
	}

	// reset the block writer, block stats will get reset when a new blockWriter is created
	s.blockWriter = nil

	s.currentByteOffset += uint64(bytesWritten)
	return nil
}

// Close finishes writing the segment file by writing the final metadata to the file and closing the writer.
//
// Once this has completed then the segment is considered durably stored.
//
// Returns the size of the file, the metadata bytes (useful for caching)
func (s *SegmentWriter) Close() (uint64, []byte, error) {
	// flush the current block if needed
	if s.blockWriter != nil {
		err := s.flushCurrentDataBlock()
		if err != nil {
			return 0, nil, fmt.Errorf("error in flushCurrentDataBlock: %w", err)
		}
	}

	if len(s.blockIndex) == 0 {
		return 0, nil, ErrNoRows
	}

	// write the meta block
	metaBlockBytes := s.generateMetaBlock()
	bytesWritten, err := s.externalWriter.Write(metaBlockBytes)
	if err != nil {
		return 0, nil, fmt.Errorf("error writing meta block to external writer: %w", err)
	}
	if bytesWritten != len(metaBlockBytes) {
		return 0, nil, fmt.Errorf("%w (meta block) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, len(metaBlockBytes), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)
	metaBlockStartOffset := s.currentByteOffset

	// Write the meta block offset
	bytesWritten, err = s.externalWriter.Write(binary.LittleEndian.AppendUint64([]byte{}, metaBlockStartOffset))
	if err != nil {
		return 0, nil, fmt.Errorf("error writing meta block offset to external writer: %w", err)
	}
	if bytesWritten != 8 {
		return 0, nil, fmt.Errorf("%w (meta block offset) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, len(metaBlockBytes), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)

	// Write the meta block hash
	bytesWritten, err = s.externalWriter.Write(binary.LittleEndian.AppendUint64([]byte{}, xxhash.Sum64(metaBlockBytes)))
	if err != nil {
		return 0, nil, fmt.Errorf("error writing block hash bytes to external writer: %w", err)
	}
	if bytesWritten != 8 {
		return 0, nil, fmt.Errorf("%w (meta block offset) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, len(metaBlockBytes), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)

	// Write the segment file version
	bytesWritten, err = s.externalWriter.Write([]byte{1})
	if err != nil {
		return 0, nil, fmt.Errorf("error writing final segment bytes to external writer: %w", err)
	}
	if bytesWritten != 1 {
		return 0, nil, fmt.Errorf("%w (meta block offset) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, 1, bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)

	// close the writer so it can't be reused
	s.closed = true

	return s.currentByteOffset, metaBlockBytes, nil
}

func (s *SegmentWriter) generateMetaBlock() []byte {
	var metaBlock bytes.Buffer

	// write the first and last key
	firstKey := s.blockIndex[0].firstKey
	metaBlock.Write(binary.LittleEndian.AppendUint16([]byte{}, uint16(len(firstKey))))
	metaBlock.Write(firstKey)
	metaBlock.Write(binary.LittleEndian.AppendUint16([]byte{}, uint16(len(s.lastKey))))
	metaBlock.Write(s.lastKey)

	// write the bloom filter type and bloom filter (if using it)
	if s.options.BloomFilter != nil {
		metaBlock.Write([]byte{1}) // using bloom filter
		var bloomBuffer bytes.Buffer
		s.options.BloomFilter.WriteTo(&bloomBuffer)
		metaBlock.Write(binary.LittleEndian.AppendUint64([]byte{}, uint64(bloomBuffer.Len()))) // write byte length
		metaBlock.Write(bloomBuffer.Bytes())                                                   // write bloom filter
	} else {
		metaBlock.Write([]byte{0}) // not using bloom filter
	}

	// write the compression
	useZSTD := s.options.ZSTDCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.LZ4Compression
	if useZSTD {
		metaBlock.Write([]byte{1})
	} else if useLZ4 {
		metaBlock.Write([]byte{2})
	} else {
		metaBlock.Write([]byte{0})
	}

	// write 0 byte to indicate not a partitioned block index
	metaBlock.Write([]byte{0})

	// write the number of block index entries
	metaBlock.Write(binary.LittleEndian.AppendUint64([]byte{}, uint64(len(s.blockIndex))))

	// write the block index items
	for _, block := range s.blockIndex {
		metaBlock.Write(block.toBytes())
	}

	return metaBlock.Bytes()
}

func (s *SegmentWriter) generateBlockIndex() []byte {
	panic("todo")
}
