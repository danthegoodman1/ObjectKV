package snapshot_reader

import (
	"bytes"
	"github.com/danthegoodman1/objectkv/sst"
	"github.com/google/btree"
	"sync"
)

type (
	Reader struct {
		segmentIDTree  *btree.BTreeG[SegmentRecord]
		blockRangeTree *btree.BTreeG[SegmentRecord]
		indexMu        *sync.RWMutex
		readerFactory  SegmentReaderFactoryFunc
	}

	// SegmentReaderFactoryFunc is used to create the readers for segment files. May be used to read data or metadata.
	SegmentReaderFactoryFunc func(record SegmentRecord) (*sst.SegmentReader, error)
)

const (
	DirectionForward = iota
	DirectionReverse
)

func NewReader(f SegmentReaderFactoryFunc) *Reader {
	sr := &Reader{
		segmentIDTree: btree.NewG[SegmentRecord](2, func(a, b SegmentRecord) bool {
			return a.ID < b.ID
		}),
		blockRangeTree: btree.NewG[SegmentRecord](2, func(a, b SegmentRecord) bool {
			// safe to do off only first key since last key >= first key always
			return bytes.Compare(a.Metadata.FirstKey, b.Metadata.FirstKey) < 0
		}),
		indexMu: &sync.RWMutex{},
	}

	return sr
}

// UpdateSegments will obtain a write lock over segment indexes, and perform all the modifications at once.
// This allows you to atomically drop and add segment files for use cases like compaction.
//
// Drop runs before add.
func (r *Reader) UpdateSegments(add []SegmentRecord, drop []SegmentRecord) {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()
	// todo handle deletes first
	// todo lookup in segment tree
	// todo drop from segment tree
	// todo drop from block range tree

	// todo handle adds
	// todo add to segment tree
	// todo add to block range tree
}

// GetRow will fetch a single row, returning sst.ErrNoRows if not found.
//
// Runs on a snapshot of segments when invoked, can run concurrently with segment updates.
func (r *Reader) GetRow(key []byte) ([]byte, error) {
	// todo see sst.SegmentReader.GetRow impl
	// todo figure out relevant blocks
	// todo check blocks in order of segment (asc level, desc ID)
	panic("todo")
}

// GetRange will fetch a range of rows up to a limit, starting from some direction.
// Internally it uses RowIter, and is a convenience wrapper around it.
//
// Runs on a snapshot of segments when invoked, can run concurrently with segment updates.
//
// See sst.UnboundStart and sst.UnboundEnd helper vars
func (r *Reader) GetRange(start []byte, end []byte, limit, direction int) ([]sst.KVPair, error) {
	// todo see sst.SegmentReader.GetRange impl
	// todo get row iters for all potential blocks
	// todo likely just a convenience wrapper around row iterator?
	// todo iterate on rows from segments in order of (asc level, desc ID),
	//  interleaving and skipping already read values, keeping track of deletes
	panic("todo")
}

// RowIter creates a new row iter for a given range.
//
// See sst.UnboundStart and sst.UnboundEnd helper vars
func (r *Reader) RowIter(start []byte, end []byte, direction int) *Iter {
	// todo figure out blocks needed to read from snapshot
	return &Iter{
		reader: r,
	}
}
