package snapshot_reader

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/danthegoodman1/objectkv/sst"
	"github.com/google/btree"
	"golang.org/x/sync/errgroup"
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
//
// The minimum information to have within a SegmentRecord is the ID and Metadata.FirstKey
func (r *Reader) UpdateSegments(add []SegmentRecord, drop []SegmentRecord) {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	// handle deletes first
	for _, toDrop := range drop {
		_, found := r.segmentIDTree.Delete(toDrop)
		if !found {
			continue
		}
		_, found = r.blockRangeTree.Delete(toDrop)
		if !found {
			// todo log warning or return error?
		}
	}

	// handle adds
	for _, toAdd := range add {
		r.segmentIDTree.ReplaceOrInsert(toAdd)
		r.blockRangeTree.ReplaceOrInsert(toAdd)
	}
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
			return nil, fmt.Errorf("error running reader factory for segment level=%d id=%s: %w", segment.Level, segment.ID, err)
		}
		defer reader.Close()

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

	// get all potential blocks
	possibleSegments := r.getPossibleSegmentsForRange(start, end)

	if len(possibleSegments) == 0 {
		// exit early
		return nil, nil
	}

	// sort them based on level, id if level 0, then direction
	sort.Slice(possibleSegments, func(i, j int) bool {
		if possibleSegments[i].Level != possibleSegments[j].Level {
			// ascending by level
			return possibleSegments[i].Level < possibleSegments[j].Level
		}

		// If both levels are 0, sort by ID to ensure that we see the newest L0 segment first
		if possibleSegments[i].Level == 0 && possibleSegments[j].Level == 0 {
			// desc by ID, we assume that there are no duplicates
			return possibleSegments[i].ID > possibleSegments[j].ID
		}

		// Sort by FirstKey or LastKey based on the direction
		if direction == sst.DirectionAscending {
			// ascending by FirstKey
			return bytes.Compare(possibleSegments[i].Metadata.FirstKey, possibleSegments[j].Metadata.FirstKey) < 0
		}
		// otherwise descending by LastKey
		return bytes.Compare(possibleSegments[i].Metadata.LastKey, possibleSegments[j].Metadata.LastKey) > 0
	})

	// get row iters for all possible segments
	segmentIters := make([]sst.RowIter, len(possibleSegments))
	cursors := make([]sst.KVPair, len(possibleSegments)) // a buffer for the next key
	for i, segment := range possibleSegments {
		// todo add concurrency to this
		reader, err := r.readerFactory(segment)
		if err != nil {
			return nil, fmt.Errorf("error in r.readerFactor for segment %s: %w", segment.ID, err)
		}

		// Close all the readers at the end
		defer reader.Close()

		iter, err := reader.RowIter(direction)
		if err != nil {
			return nil, fmt.Errorf("error in reader.RowIter for segment %s: %w", segment.ID, err)
		}

		segmentIters[i] = *iter
		pair, err := segmentIters[i].Next()
		if err != nil {
			return nil, fmt.Errorf("error in sst.RowIter.Next() for segment %s: %w", segment.ID, err)
		}
		cursors[i] = pair
	}

	rows := make([]sst.KVPair, limit)
	addedRowIndex := 0
	var lastKey []byte // safe because key can never be empty
	for {
		// get the index of the cursors with the next value in the direction we want
		nextIndexes := findMaxIndexes(cursors, func(a, b sst.KVPair) int {
			return firstValue(a.Key, b.Key, direction)
		})
		if len(nextIndexes) == 0 {
			return nil, ErrNoNextIndexFound
		}

		// Check if the first value is a L0 tombstone
		if possibleSegments[nextIndexes[0]].Level == 0 && cursors[nextIndexes[0]].Value == nil {
			// this row is deleted, roll forward all matching indexes and continue
			// roll them forward concurrently
			g := errgroup.Group{}
			for _, ind := range nextIndexes {
				g.Go(func() (err error) {
					cursors[ind], err = segmentIters[ind].Next()
					if err != nil {
						return fmt.Errorf("error in sst.RowIter.Next() for segment %s: %w", possibleSegments[ind].ID, err)
					}
					return
				})
				err := g.Wait()
				if err != nil {
					return nil, fmt.Errorf("error in errgroup.Group.Wait: %w", err)
				}
			}
			continue
		}

		// Get the first value from our cursors
		row := cursors[nextIndexes[0]]
		if !bytes.Equal([]byte{}, lastKey) && bytes.Equal(row.Key, lastKey) {
			// this is the same value, there will be no more values in this direction
			break
		}

		// verify that this row is in our range
		if bytes.Compare(start, row.Key) > 0 || bytes.Compare(end, row.Key) < 0 {
			break
		}

		// otherwise we have the next value in the range
		lastKey = row.Key
		rows[addedRowIndex] = row
		addedRowIndex++
		if addedRowIndex >= limit-1 {
			// we have hit the limit
			break
		}

		// roll forward all matching indexes
		g := errgroup.Group{}
		for _, ind := range nextIndexes {
			g.Go(func() (err error) {
				cursors[ind], err = segmentIters[ind].Next()
				if err != nil {
					return fmt.Errorf("error in sst.RowIter.Next() for segment %s: %w", possibleSegments[ind].ID, err)
				}
				return
			})
			err := g.Wait()
			if err != nil {
				return nil, fmt.Errorf("error in errgroup.Group.Wait: %w", err)
			}
		}
	}

	return rows, nil
}

var ErrNoNextIndexFound = errors.New("did not find a next index, this is a bug, please report")

// firstValue returns 1 if a is first by direction, 0 if they are the same, -1 if b is more significant.
// like bytes.Compare but takes the direction into account.
//
// If DirectionAscending, it returns the smaller value. If DirectionDescending, it returns the larger value.
func firstValue(a, b []byte, direction int) int {
	r := bytes.Compare(a, b)
	if r == 0 {
		return 0
	}
	if direction == sst.DirectionDescending {
		if r > 0 {
			return 1
		}
		return -1
	}

	// otherwise assume descending
	if r < 0 {
		return 1
	}
	return -1
}

// compareFunc is a type for the comparison function, expects the same format results as bytes.Compare
type compareFunc[T any] func(a, b T) int

// findMaxIndexes is a generic function to find indexes of the largest value
func findMaxIndexes[T any](arr []T, compare compareFunc[T]) []int {
	if len(arr) == 0 {
		return nil
	}

	max := arr[0]
	indexes := []int{0}

	for i := 1; i < len(arr); i++ {
		cmp := compare(arr[i], max)
		if cmp > 0 {
			max = arr[i]
			indexes = []int{i} // reset indexes slice
		} else if cmp == 0 {
			indexes = append(indexes, i)
		}
	}

	return indexes
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
