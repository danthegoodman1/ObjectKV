package snapshot_reader

import (
	"bytes"
	"github.com/danthegoodman1/objectkv/sst"
	"github.com/google/btree"
	"sync"
)

type (
	SnapshotReader struct {
		segmentIDTree  *btree.BTreeG[SegmentRecord]
		blockRangeTree *btree.BTreeG[SegmentRecord]
		indexMu        *sync.RWMutex
		readerFactory  ReaderFactoryFunc
	}

	// ReaderFactoryFunc is used to create the readers for segment files. May be used to read data or metadata.
	ReaderFactoryFunc func(record SegmentRecord) (*sst.SegmentReader, error)
)

const (
	DirectionForward = iota
	DirectionReverse
)

func NewSnapshotReader(f ReaderFactoryFunc) *SnapshotReader {
	sr := &SnapshotReader{
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

// AddSegment will add a Segment to the index, and instantly becomes available for reading.
// Segments should only be added once fully durable and available to read.
//
// To reduce memory usage, you can opt to use a nil value for the sst.Metadata.BlockIndex,
// and SnapshotReader will fetch metadata on-demand and use for data block-level filtering.
func (sr *SnapshotReader) AddSegment(record SegmentRecord) {
	sr.indexMu.Lock()
	defer sr.indexMu.Unlock()
	// todo add to segment tree
	// todo add to block range tree
	panic("todo")
}

func (sr *SnapshotReader) DropSegment(segmentID string) {
	sr.indexMu.Lock()
	defer sr.indexMu.Unlock()
	// todo lookup in segment tree
	// todo drop from segment tree
	// todo drop from block range tree
	panic("todo")
}

func (sr *SnapshotReader) GetRow(key []byte) ([]byte, error) {
	// todo see sst.SegmentReader.GetRow impl
	// todo figure out relevant blocks
	// todo if no metadata, fetch on-demand
	// todo check blocks in order of segment (asc level, desc ID)
	panic("todo")
}

func (sr *SnapshotReader) GetRange(start []byte, end []byte, limit, direction int) ([]sst.KVPair, error) {
	// todo see sst.SegmentReader.GetRange impl
	// todo if no metadata, fetch on-demand
	// todo get row iters for all potential blocks
	// todo iterate on rows from segments in order of (asc level, desc ID),
	//  interleaving and skipping already read values, keeping track of deletes
	panic("todo")
}

func (sr *SnapshotReader) RowIter(start []byte, direction int) {

}
