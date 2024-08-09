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
		currentBlockSize     uint64
		currentRawBlockSize  uint64
		currentBlockStartKey []byte
		blockBuffer          *bytes.Buffer // the buffer for the (un)compressed block
		blockWriter          io.Writer     // write to the blockBuffer with optional compression

		// writes to actual destination (S3 &/ file)
		externalWriter io.Writer

		currentByteOffset uint64                             // where we are in the file currently, used for block index
		blockIndex        map[[math.MaxUint16]byte]blockStat // todo, either a tree or https://github.com/wk8/go-ordered-map
		lastKey           []byte

		options SegmentWriterOptions

		closed bool
	}
)

// NewSegmentWriter creates a new segment writer and opens the file(s) for writing.
//
// A segment writer can never be reused, and is not thread safe.
func NewSegmentWriter(path string, writer io.Writer, opts SegmentWriterOptions) SegmentWriter {
	sw := SegmentWriter{
		options:        opts,
		externalWriter: writer,
		blockIndex:     map[[math.MaxUint16]byte]blockStat{},
	}

	return sw
}

var (
	ErrWriterClosed           = errors.New("segment writer already closed")
	ErrUnexpectedBytesWritten = errors.New("unexpected number of bytes written")
	ErrKeyTooLarge            = errors.New("key too large, must be <= max uint16 bytes")
	ErrValueTooLarge          = errors.New("value too large, must be <= max uin32 bytes")
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
	useZSTD := s.options.zstdCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.lz4Compression
	if s.blockWriter == nil {
		// Ensure we are at a base state
		s.currentBlockStartKey = key
		s.currentBlockSize = 0
		s.currentRawBlockSize = 0
		s.blockBuffer = &bytes.Buffer{}

		// create the writer if it doesn't exist, using the correct writer based on compression
		// todo check lz4 compression
		if useZSTD {
			enc, err := zstd.NewWriter(s.blockBuffer)
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
	binary.LittleEndian.PutUint32(rowBuf[2:6], uint32(len(key)))
	copy(rowBuf[8:], key)
	copy(rowBuf[8+len(key):], val)

	bytesWritten, err := s.blockWriter.Write(rowBuf)
	if err != nil {
		return fmt.Errorf("error in s.blockWriter.Write (zstd=%t, lz4=%t): %w", useZSTD, useLZ4, err)
	}
	s.currentBlockSize += uint64(bytesWritten)
	s.currentRawBlockSize += uint64(len(rowBuf))

	if s.options.bloomFilter != nil {
		// store the row in the bloom filter if needed
		s.options.bloomFilter.Add(key)
	}

	if s.currentBlockSize >= s.options.dataBlockThresholdBytes {
		err = s.flushCurrentDataBlock()
		if err != nil {
			return fmt.Errorf("error in flushCurrentDataBlock: %w", err)
		}
	}

	return nil
}

func (s *SegmentWriter) flushCurrentDataBlock() error {
	useZSTD := s.options.zstdCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.lz4Compression

	if remainder := s.currentBlockSize % s.options.dataBlockSize; remainder > 0 {
		// write the (padded min) multiple of 4k block to the file after compression
		bytesWritten, err := s.blockBuffer.Write(make([]byte, remainder))
		if err != nil {
			return fmt.Errorf("error writing padding to externalWriter: %w", err)
		}
		if uint64(bytesWritten) != remainder {
			return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, remainder, bytesWritten)
		}
	}

	blockBytes := s.blockBuffer.Bytes()

	// capture a blockHash of the final block bytes
	blockHash := xxhash.Sum64(blockBytes)

	// flush the block buffer
	bytesWritten, err := s.externalWriter.Write(blockBytes)
	if err != nil {
		return fmt.Errorf("error writing raw block writer bytes to external writer: %w", err)
	}
	if bytesWritten != s.blockBuffer.Len() {
		return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, s.blockBuffer.Len(), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)

	// write the metadata to memory for the block start with offset and first key
	stat := blockStat{
		offset:   s.currentByteOffset,
		rawBytes: s.currentRawBlockSize,
		hash:     blockHash,
	}
	if useZSTD || useLZ4 {
		stat.compressedBytes = s.currentBlockSize
	}
	s.blockIndex[[math.MaxUint16]byte(s.currentBlockStartKey)] = stat

	// reset the block writer, block stats will get reset when a new blockWriter is created
	s.blockWriter = nil

	return nil
}

// Close finishes writing the segment file by writing the final metadata to the file and closing the writer.
//
// Once this has completed then the segment is considered durably stored.
//
// Returns the size of the file
func (s *SegmentWriter) Close() (uint64, error) {
	// flush the current block if needed
	if s.blockWriter != nil {
		err := s.flushCurrentDataBlock()
		if err != nil {
			return 0, fmt.Errorf("error in flushCurrentDataBlock: %w", err)
		}
	}

	// write the meta block
	metaBlockBytes := s.generateMetaBlock()
	bytesWritten, err := s.externalWriter.Write(metaBlockBytes)
	if err != nil {
		return 0, fmt.Errorf("error writing meta block to external writer: %w", err)
	}
	if bytesWritten != len(metaBlockBytes) {
		return 0, fmt.Errorf("%w (meta block) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, len(metaBlockBytes), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)
	metaBlockStartOffset := s.currentByteOffset

	// write the first and last value of the file
	finalSegmentBytes := make([]byte, 2+len(s.lastKey)+8) // uint16 last key length + last key bytes + meta block start offset
	binary.LittleEndian.PutUint16(finalSegmentBytes[0:2], uint16(len(s.lastKey)))
	copy(finalSegmentBytes[2:], s.lastKey)
	binary.LittleEndian.PutUint64(finalSegmentBytes[2+len(s.lastKey):], metaBlockStartOffset)

	bytesWritten, err = s.externalWriter.Write(finalSegmentBytes)
	if err != nil {
		return 0, fmt.Errorf("error writing final segment bytes to external writer: %w", err)
	}
	if bytesWritten != len(metaBlockBytes) {
		return 0, fmt.Errorf("%w (meta block) - expected=%d wrote=%d", ErrUnexpectedBytesWritten, len(metaBlockBytes), bytesWritten)
	}
	s.currentByteOffset += uint64(bytesWritten)

	// close the writer so it can't be reused
	s.closed = true

	return s.currentByteOffset, nil
}

func (s *SegmentWriter) generateMetaBlock() []byte {
	// todo write the block index type and block index
	// todo write the bloom filter type and bloom filter (if using it)
	panic("todo")
}

func (s *SegmentWriter) generateBlockIndex() []byte {
	panic("todo")
}
