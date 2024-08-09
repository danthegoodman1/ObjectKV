package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"math"
)

type SegmentWriter struct {
	currentBlockSize     int
	currentBlockOffset   int
	currentBlockStartKey []byte
	// Block buffer depends on compression setting
	rawBlockBuffer bytes.Buffer
	blockWriter    io.Writer

	// writes to actual destination (S3 &/ file)
	externalWriter io.Writer

	// index of (firstKey, (startOffset, compressed size, decompressed size))
	blockIndex any // todo, either a tree or https://github.com/wk8/go-ordered-map
	lastKey    []byte

	options SegmentWriterOptions

	closed bool
}

// NewSegmentWriter creates a new segment writer and opens the file(s) for writing.
//
// A segment writer can never be reused.
func NewSegmentWriter(path string, writer io.Writer, opts SegmentWriterOptions) SegmentWriter {
	sw := SegmentWriter{
		rawBlockBuffer: bytes.Buffer{},
		options:        opts,
		externalWriter: writer,
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
		// create the writer if it doesn't exist, using the correct writer based on compression
		// todo check lz4 compression
		if useZSTD {
			enc, err := zstd.NewWriter(s.externalWriter)
			if err != nil {
				return fmt.Errorf("error in zstd.NewWriter: %w", err)
			}
			s.blockWriter = enc
		} else {
			s.blockWriter = s.externalWriter
		}
		s.currentBlockStartKey = key
		s.currentBlockSize = 0
	}

	// update the key tracking for final write
	s.lastKey = key

	// write the row for the current block into the buffer
	rowBuf := make([]byte, 6+len(key)+len(val))
	binary.LittleEndian.PutUint16(rowBuf[0:2], uint16(len(key)))
	binary.LittleEndian.PutUint32(rowBuf[2:6], uint32(len(key)))
	copy(rowBuf[8:], key)
	copy(rowBuf[8+len(key):], val)

	bytesWritten, err := s.rawBlockBuffer.Write(rowBuf)
	if err != nil {
		return fmt.Errorf("error in s.blockWriter.Write (zstd=%t, lz4=%t): %w", useZSTD, useLZ4, err)
	}
	s.currentBlockOffset += bytesWritten
	s.currentBlockSize += bytesWritten

	if s.options.bloomFilter != nil {
		// store the row in the bloom filter if needed
		s.options.bloomFilter.Add(key)
	}

	if s.currentBlockOffset < s.options.dataBlockThresholdBytes {
		// todo what ever is needed to continue if anything
		return nil
	}

	// Otherwise we tripped the block threshold and need to flush the data block

	if remainder := s.currentBlockSize % s.options.dataBlockSize; remainder > 0 {
		// write the (padded min) multiple of 4k block to the file after compression
		bytesWritten, err = s.rawBlockBuffer.Write(make([]byte, remainder))
		if err != nil {
			return fmt.Errorf("error writing padding to rawBlockBuffer: %w", err)
		}
		if bytesWritten != remainder {
			return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, remainder, bytesWritten)
		}
	}

	// todo flush the rawBlockBuffer to the blockWriter (writes to flush writer)
	// todo write the metadata to memory for the block start with offset and first key

	// reset block writing state
	s.currentBlockOffset = 0
	s.blockWriter = nil
	s.rawBlockBuffer = bytes.Buffer{}
	panic("todo")
}

// Close finishes writing the segment file by writing the final metadata to the file and closing the writer.
//
// Once this has completed then the segment is considered durably stored.
func (s *SegmentWriter) Close() error {
	// todo write the block index
	// todo write the bloom filter if using it
	// todo record the first and last value of the file
	// todo write the offset for where in the file the metadata starts
	// todo close the remote file
	// todo close the cached file if exists
	s.closed = true
	panic("todo")
}
