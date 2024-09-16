package snapshot_reader

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github.com/danthegoodman1/objectkv/sst"
	"github.com/google/btree"
	"golang.org/x/sync/errgroup"
	"io"
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

func blockRangeLessFunc(a, b SegmentRecord) bool {
	// Compare FirstKey first
	cmp := bytes.Compare(a.Metadata.FirstKey, b.Metadata.FirstKey)
	if cmp != 0 {
		return cmp < 0
	}

	// We are looking up where a key belongs in the range ("unbound" range)
	if len(a.Metadata.LastKey) == 0 {
		return false
	}
	if len(b.Metadata.LastKey) == 0 {
		return true
	}

	// Check the last key, reverse order, so the largest range comes first when descending
	cmp = bytes.Compare(a.Metadata.LastKey, b.Metadata.LastKey)
	if cmp != 0 {
		return cmp < 0
	}

	// this is a bit of a hack to prevent duplicates, while allowing for search
	// if the ID is blank then we are searching for potentials, so return the opposite
	if a.ID == "" {
		return false
	}
	if b.ID == "" {
		return true
	}

	// If FirstKey and LastKey is the same, compare ID (so everything is unique)
	return a.ID < b.ID
}

func NewReader(f SegmentReaderFactoryFunc) *Reader {
	sr := &Reader{
		segmentIDTree: btree.NewG[SegmentRecord](2, func(a, b SegmentRecord) bool {
			return a.ID < b.ID
		}),
		blockRangeTree: btree.NewG[SegmentRecord](2, blockRangeLessFunc),
		indexMu:        &sync.RWMutex{},
		readerFactory:  f,
	}

	return sr
}

// UpdateSegments will obtain a write lock over segment indexes, and perform all the modifications at once.
// This allows you to atomically drop and add segment files for use cases like compaction.
//
// Drop runs before add.
//
// The minimum information to have within a SegmentRecord is the ID, Metadata.FirstKey, Metadata.LastKey
func (r *Reader) UpdateSegments(add []SegmentRecord, drop []SegmentRecord) {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	// handle deletes first
	for _, toDrop := range drop {
		_, found := r.segmentIDTree.Delete(toDrop)
		if !found {
			continue
		}
		r.blockRangeTree.Delete(toDrop)
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
	defer r.indexMu.RUnlock()

	// Descend from the key until we hit something too small
	r.blockRangeTree.DescendLessOrEqual(SegmentRecord{
		Metadata: sst.SegmentMetadata{FirstKey: key},
	}, func(record SegmentRecord) bool {
		keyInRange := bytes.Compare(key, record.Metadata.FirstKey) >= 0 && bytes.Compare(key, record.Metadata.LastKey) <= 0
		if keyInRange {
			possibleSegments = append(possibleSegments, record)
		}
		return keyInRange
	})

	return possibleSegments
}

// getPossibleSegmentsForRange returns all possible segments a range of keys could live in
func (r *Reader) getPossibleSegmentsForRange(start, end []byte) []SegmentRecord {
	// NOTE maybe we can pre-create this to segment size
	// to exchange higher mem for fewer allocations?
	var possibleSegments []SegmentRecord
	r.indexMu.RLock()
	defer r.indexMu.RUnlock()

	// Descend from the key until we hit something too small
	r.blockRangeTree.DescendLessOrEqual(SegmentRecord{
		Metadata: sst.SegmentMetadata{FirstKey: end},
	}, func(record SegmentRecord) bool {
		// easier to check the conditions it can't overlap in
		keyInRange := !(bytes.Compare(start, record.Metadata.LastKey) > 0 || bytes.Compare(end, record.Metadata.FirstKey) < 0)
		if keyInRange {
			possibleSegments = append(possibleSegments, record)
		}
		return keyInRange
	})

	return possibleSegments
}

var ErrInvalidRange = errors.New("invalid range")

// GetRange will fetch a range of rows up to a limit, starting from some direction.
// Internally it uses RowIter, and is a convenience wrapper around it.
//
// `end` must be greater than `start`, with the range [start, end): start inclusive, end exclusive when
// sst.DirectionAscending and [end, start) when sst.DirectionDescending. This means you can paginate without
// worrying about overlap.
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
	startRange := start                                  // what to seek to
	if direction == sst.DirectionDescending {
		startRange = end
	}

	for i, segment := range possibleSegments {
		g := errgroup.Group{}
		g.Go(func() error {
			reader, err := r.readerFactory(segment)
			if err != nil {
				return fmt.Errorf("error in r.readerFactor for segment %s: %w", segment.ID, err)
			}

			iter, err := reader.RowIter(direction)
			if err != nil {
				return fmt.Errorf("error in reader.RowIter for segment %s: %w", segment.ID, err)
			}

			// Seek it
			// todo current problem is that when we seek to key000 for segment that starts at key001 it hits nil stat bc nothing less and it jumps to end
			err = iter.Seek(startRange)
			if err != nil {
				return fmt.Errorf("error in iter.Seek to start range for segment %s: %w", segment.ID, err)
			}

			segmentIters[i] = *iter
			pair, err := segmentIters[i].Next()
			if err != nil {
				return fmt.Errorf("error in sst.RowIter.Next() after start range for segment %s: %w", segment.ID, err)
			}
			cursors[i] = pair
			return nil
		})
		err := g.Wait()
		if err != nil {
			return nil, fmt.Errorf("error setting up segment iterators: %w", err)
		}
	}

	for _, iter := range segmentIters {
		// Close all the readers at the end
		defer iter.CloseReader()
	}

	rows := make([]sst.KVPair, limit)
	addedRowIndex := 0
	var lastKey []byte // sst.KVPair.Key can never be empty, so if this is empty we know we haven't set it yet
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
						return fmt.Errorf("error in sst.RowIter.Next() when rolling forward non matching for segment %s: %w", possibleSegments[ind].ID, err)
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
		if direction == sst.DirectionAscending && bytes.Compare(row.Key, end) >= 0 {
			break
		}
		if direction == sst.DirectionDescending && bytes.Compare(row.Key, start) <= 0 {
			break
		}

		// otherwise we have the next value in the range
		lastKey = row.Key
		rows[addedRowIndex] = row
		addedRowIndex++
		if addedRowIndex >= limit {
			// we have hit the limit
			break
		}

		// roll forward all matching indexes
		g := errgroup.Group{}
		for _, ind := range nextIndexes {
			g.Go(func() (err error) {
				newCursor, err := segmentIters[ind].Next()
				if errors.Is(err, io.EOF) {
					// We can't load any more, leave the old value so we don't have any issues
					return nil
				}
				if err != nil {
					return fmt.Errorf("error in sst.RowIter.Next() for rolling forward matching for segment %s: %w", possibleSegments[ind].ID, err)
				}

				cursors[ind] = newCursor
				return
			})
			err := g.Wait()
			if err != nil {
				return nil, fmt.Errorf("error in errgroup.Group.Wait: %w", err)
			}
		}
	}

	// Only return a slice of what actually returned
	return rows[:addedRowIndex], nil
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

// intCompareFunc is a type for the comparison function, expects the same format results as bytes.Compare
type intCompareFunc[T any] func(a, b T) int

// findMaxIndexes is a generic function to find indexes of the largest value
func findMaxIndexes[T any](arr []T, compare intCompareFunc[T]) []int {
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

// RowIter creates a new row iter for a given range. Internally, it manages multiple
// GetRange requests and buffers their response, so it's very much a convenience API
// and provides no performance benefits.
//
// See sst.UnboundStart and sst.UnboundEnd helper vars.
func (r *Reader) RowIter(start []byte, direction int, opts ...IterOption) *Iter {
	iter := &Iter{
		reader:    r,
		lastKey:   start,
		direction: direction,
		options:   defaultIterOptions,
		rowBuffer: list.New(), // give an initial list so it knows to fill
	}

	for _, opt := range opts {
		opt(&iter.options)
	}

	return iter
}
