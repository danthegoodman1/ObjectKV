package sst

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
)

type SegmentWriter struct {
	currentBlockSize     int
	currentBlockOffset   int
	currentBlockStartKey []byte
	// Block buffer depends on compression setting
	rawBlockBuffer bytes.Buffer
	blockWriter    io.Writer
	blockIndex     any // todo, either a tree or https://github.com/wk8/go-ordered-map
	lastBlockKey   []byte
	bloomFilter    any

	// options
	localCacheDir        *string
	zstdCompressionLevel int // if not 0, then use this
	lz4Compression       bool

	closed bool
}

// NewSegmentWriter creates a new segment writer and opens the file(s) for writing.
//
// A segment writer can never be reused.
func NewSegmentWriter(path string, opts ...SegmentWriterOption) SegmentWriter {
	sw := SegmentWriter{
		rawBlockBuffer: bytes.Buffer{},
	}
	for _, opt := range opts {
		opt(&sw)
	}

	return sw
}

var ErrWriterClosed = errors.New("segment writer already closed")

// WriteRow writes a given row to the segment. Cannot write after the writer is closed.
//
// It is expected that rows are written in order.
func (s *SegmentWriter) WriteRow(key, val []byte) error {
	if s.closed {
		return ErrWriterClosed
	}
	useZSTD := s.zstdCompressionLevel > 0
	useLZ4 := !useZSTD && s.lz4Compression
	if s.blockWriter == nil {
		// create the writer if it doesn't exist, using the correct writer based on compression
		// todo check lz4 compression
		if useZSTD {
			enc, err := zstd.NewWriter(&s.rawBlockBuffer)
			if err != nil {
				return fmt.Errorf("error in zstd.NewWriter: %w", err)
			}
			s.blockWriter = enc
		} else {
			s.blockWriter = &s.rawBlockBuffer
		}
		s.currentBlockStartKey = key
		s.currentBlockSize = 0
	}

	// update the key tracking for metadata
	s.lastBlockKey = key

	// write the row for the current block into the buffer
	bytesWritten, err := s.blockWriter.Write([]byte{}) // todo
	if err != nil {
		return fmt.Errorf("error in s.blockWriter.Write (zstd=%t, lz4=%t): %w", useZSTD, useLZ4, err)
	}
	s.currentBlockOffset += bytesWritten

	// todo store the row in the bloom filter if needed

	if s.currentBlockOffset < 4096 {
		// todo
		return nil
	}

	// todo write the (padded min) multiple of 4k block to the file
	// todo flush the block once 4k is tripped
	// todo update the current block offset and clear writer and buffer
	// todo write the metadata to memory for the block start
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
	s.closed = true
	panic("todo")
}
