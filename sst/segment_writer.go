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
	// index of (firstKey, (startOffset, sizeOffset))
	blockIndex   any // todo, either a tree or https://github.com/wk8/go-ordered-map
	lastBlockKey []byte

	options SegmentWriterOptions

	closed bool
}

// NewSegmentWriter creates a new segment writer and opens the file(s) for writing.
//
// A segment writer can never be reused.
func NewSegmentWriter(path string, opts SegmentWriterOptions) SegmentWriter {
	sw := SegmentWriter{
		rawBlockBuffer: bytes.Buffer{},
		options:        opts,
	}

	return sw
}

var (
	ErrWriterClosed           = errors.New("segment writer already closed")
	ErrUnexpectedBytesWritten = errors.New("unexpected number of bytes written")
)

// WriteRow writes a given row to the segment. Cannot write after the writer is closed.
//
// It is expected that rows are written in order.
func (s *SegmentWriter) WriteRow(key, val []byte) error {
	if s.closed {
		return ErrWriterClosed
	}
	useZSTD := s.options.zstdCompressionLevel > 0
	useLZ4 := !useZSTD && s.options.lz4Compression
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
	bytesWritten, err := s.blockWriter.Write([]byte{}) // todo write the key and value
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

	if remainder := s.currentBlockSize % 4096; remainder > 0 {
		// write the (padded min) multiple of 4k block to the file after compression
		bytesWritten, err = s.rawBlockBuffer.Write(make([]byte, remainder))
		if err != nil {
			return fmt.Errorf("error writing padding to rawBlockBuffer: %w", err)
		}
		if bytesWritten != remainder {
			return fmt.Errorf("%w - expected=%d wrote=%d", ErrUnexpectedBytesWritten, remainder, bytesWritten)
		}
	}

	// todo flush the block
	// todo update the current block offset and clear writer and buffer
	// todo write the metadata to memory for the block start
	// reset the block writer
	s.blockWriter = nil
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
