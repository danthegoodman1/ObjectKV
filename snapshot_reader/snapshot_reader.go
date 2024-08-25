package snapshot_reader

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/danthegoodman1/objectkv/sst"
	"github.com/google/btree"
	"sort"
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
	DirectionAscending = iota
	DirectionDescending
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
	// figure out possible segments
	possibleSegments := r.getPossibleSegmentsForKey(key)

	// Sort them in desc ID order
	sort.Slice(possibleSegments, func(i, j int) bool {
		if possibleSegments[i].Level != possibleSegments[j].Level {
			// ascending by level
			return possibleSegments[i].Level < possibleSegments[j].Level
		}
		// descending by ID
		return possibleSegments[i].ID > possibleSegments[j].ID
	})

	for _, segment := range possibleSegments {
		// generate a reader for the segment
		reader, err := r.readerFactory(segment)
		if err != nil {
			return nil, fmt.Errorf("error running reader factory for segment level=%d id=%d: %w", segment.Level, segment.ID, err)
		}

		// delegate the reader to the segment reader
		row, err := reader.GetRow(key)
		if errors.Is(err, sst.ErrNoRows) {
			// not in this segment, go to the next
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("error in reader.GetRow: %w", err)
		}

		if bytes.Equal([]byte{}, row.Value) && segment.Level == 0 {
			// this is a delete, row does not exist
			return nil, sst.ErrNoRows
			// NOTE should we panic if this is not level 0? that should never happen,
			// but it's not detrimental, but it means we are not operating as we expect!
		}

		// otherwise we have a row
		return row.Value, nil
	}

	// we never found anything
	return nil, sst.ErrNoRows
}

// getPossibleSegmentsForKey will get all segments a key could live in
func (r *Reader) getPossibleSegmentsForKey(key []byte) []SegmentRecord {
	// NOTE maybe we can pre-create this to segment size
	// to exchange higher mem for fewer allocations?
	var possibleSegments []SegmentRecord
	r.indexMu.RLock()
	defer r.indexMu.Unlock()

	// Descend from the key until we hit something too small
	r.blockRangeTree.DescendLessOrEqual(SegmentRecord{
		Metadata: sst.SegmentMetadata{FirstKey: key},
	}, func(item SegmentRecord) bool {
		lessThan := bytes.Compare(key, item.Metadata.FirstKey) < 0
		if !lessThan {
			possibleSegments = append(possibleSegments, item)
		}
		return lessThan // key is less than first key
	})

	return possibleSegments
}

// getPossibleSegmentsForRange returns all possible segments a range of keys could live in
func (r *Reader) getPossibleSegmentsForRange(start, end []byte) []SegmentRecord {
	// NOTE maybe we can pre-create this to segment size
	// to exchange higher mem for fewer allocations?
	var possibleSegments []SegmentRecord
	r.indexMu.RLock()
	defer r.indexMu.Unlock()

	// Descend from the key until we hit something too small
	r.blockRangeTree.DescendLessOrEqual(SegmentRecord{
		Metadata: sst.SegmentMetadata{FirstKey: end},
	}, func(item SegmentRecord) bool {
		lessThan := bytes.Compare(start, item.Metadata.FirstKey) < 0
		if !lessThan {
			possibleSegments = append(possibleSegments, item)
		}
		return lessThan // key is less than start key
	})

	return possibleSegments
}

var ErrInvalidRange = errors.New("invalid range")

// GetRange will fetch a range of rows up to a limit, starting from some direction.
// Internally it uses RowIter, and is a convenience wrapper around it.
//
// `end` must be greater than `start`
//
// Runs on a snapshot of segments when invoked, can run concurrently with segment updates.
//
// See sst.UnboundStart and sst.UnboundEnd helper vars
func (r *Reader) GetRange(start []byte, end []byte, limit, direction int) ([]sst.KVPair, error) {
	if bytes.Compare(start, end) >= 0 {
		return nil, fmt.Errorf("%w: end must be strictly greater than start", ErrInvalidRange)
	}
	// todo see sst.SegmentReader.GetRange impl
	// get all potential blocks
	possibleSegments := r.getPossibleSegmentsForRange(start, end)

	// sort them based on level, then direction
	sort.Slice(possibleSegments, func(i, j int) bool {
		if possibleSegments[i].Level != possibleSegments[j].Level {
			// ascending by level
			return possibleSegments[i].Level < possibleSegments[j].Level
		}

		if direction == DirectionAscending {
			// ascending by first key
			return bytes.Compare(possibleSegments[i].Metadata.FirstKey, possibleSegments[j].Metadata.FirstKey) < 0
		}
		// otherwise descending by last key
		return bytes.Compare(possibleSegments[i].Metadata.LastKey, possibleSegments[j].Metadata.LastKey) > 0
	})

	// todo get row iters for all possible segments

	// rows := make([]sst.KVPair, limit)
	// addedRows := 0
	// todo iterate on rows from segments in order of (asc level, desc ID)
	//  Get the first value (consider direction) for each iterator (cursor)
	//  take the most significant value across cursors,
	//  if competing by key take from the most significant segment
	//  if L0 has a tombstone, roll forward all iters with that same key (if exists, they should be at top)
	//  (anything that has that key next) and don't count that
	//  roll forward (.Next()) all matching cursors with the most significant key
	//  once we hit limit return the rows (they will be in order)
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
