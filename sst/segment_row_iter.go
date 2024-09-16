package sst

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
)

type (
	RowIter struct {
		statLastKey []byte
		blockRows   []KVPair
		blockRowIdx int
		s           *SegmentReader
		direction   int
		initialized bool
	}
)

const (
	DirectionAscending = iota
	DirectionDescending
)

var ErrClosed = errors.New("closed")

// Next returns io.EOF when there are no more rows. Can safely call Next after an io.EOF error, as that will be
// cached in the RowIter instance, so there is zero cost to blindly calling it (e.g. cursor logic in SnapshotReader).
// Will return ErrClosed if the respective SegmentReader is closed.
func (r *RowIter) Next() (KVPair, error) {
	if r.s.closed {
		return KVPair{}, ErrClosed
	}

	if r.blockRows != nil && r.blockRowIdx < len(r.blockRows) && r.blockRowIdx >= 0 {
		// return the row if we have them, and have not reached the end
		pair := r.blockRows[r.blockRowIdx]
		r.blockRowIdx++
		return pair, nil
	}
	// otherwise we need to load the next block's rows
	var stat *BlockStat
	if r.direction == DirectionDescending {
		// special check to make sure this is a new iter and not a Seek(UnboundStart) while DirectionDescending
		if r.statLastKey == nil && r.blockRowIdx > -1 {
			// we grab the top key
			r.statLastKey = r.s.metadata.LastKey
		}
		r.s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: r.statLastKey}, func(item BlockStat) bool {
			if bytes.Equal(r.statLastKey, item.FirstKey) {
				// keep going, this is the same key
				return true
			}

			// Otherwise we take it and exit (next stat)
			r.statLastKey = item.FirstKey
			stat = &item
			return false
		})
	} else {
		// ascending by default
		r.s.metadata.BlockIndex.AscendGreaterOrEqual(BlockStat{FirstKey: r.statLastKey}, func(item BlockStat) bool {
			if bytes.Equal(r.statLastKey, item.FirstKey) {
				// keep going, this is the same key
				return true
			}

			// Otherwise we take it and exit (next stat)
			r.statLastKey = item.FirstKey
			stat = &item
			return false
		})

	}

	if stat == nil {
		// there are no more blocks
		return KVPair{}, io.EOF
	}

	rows, err := r.s.ReadBlockWithStat(*stat)
	if err != nil {
		return KVPair{}, fmt.Errorf("error in SegmentReader.ReadBlockWithStat: %w", err)
	}

	r.blockRows = rows
	// if descending, we need to reverse the block
	if r.direction == DirectionDescending {
		slices.Reverse(r.blockRows)
	}

	r.blockRowIdx = 1
	return r.blockRows[0], nil
}

// Seek will seek up to the given key, such that any subsequent Next
// call will return greater than or equal to key (or io.EOF).
//
// Can use UnboundStart and UnboundEnd to seek to the start and end
func (r *RowIter) Seek(key []byte) error {
	// find the last block first key before this
	var stat *BlockStat
	isUnboundStart := bytes.Equal(key, UnboundStart)
	isUnboundEnd := bytes.Equal(key, UnboundEnd)
	if isUnboundStart {
		first, _ := r.s.metadata.BlockIndex.Min()
		stat = &first
	} else if isUnboundEnd {
		last, _ := r.s.metadata.BlockIndex.Max()
		stat = &last
	} else {
		r.s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: key}, func(item BlockStat) bool {
			stat = &item
			return bytes.Compare(key, item.FirstKey) <= 0
		})
	}

	missingStat := stat == nil
	var rows []KVPair
	var err error
	r.blockRowIdx = 0
	if missingStat {
		// there are no more blocks, jump to the ends
		switch r.direction {
		case DirectionAscending:
			// check if we are lower than the first key
			firstBlock, _ := r.s.metadata.BlockIndex.Min()
			if bytes.Compare(key, firstBlock.FirstKey) < 0 {
				// We are at the beginning, set to first
				stat = &firstBlock
			} else {
				// We are past the entire segment, go to the end
				lastBlock, _ := r.s.metadata.BlockIndex.Max()
				stat = &lastBlock
				r.blockRowIdx = len(rows) - 1
			}

		case DirectionDescending:
			// check if we are greater than the last key
			lastBlock, _ := r.s.metadata.BlockIndex.Max()
			rows, err = r.s.ReadBlockWithStat(lastBlock)
			if err != nil {
				return fmt.Errorf("error in ReadBlockWithState to inspect end of last block: %w", err)
			}
			if bytes.Compare(key, rows[len(rows)-1].Key) > 0 {
				// We are at the beginning, set to end
				stat = &lastBlock
			} else {
				// We are past the entire segment, go to the end
				firstBlock, _ := r.s.metadata.BlockIndex.Min()
				stat = &firstBlock
				r.blockRowIdx = len(rows) - 1
			}
		}
	} else {
		r.blockRowIdx = 0
	}

	// Set the last key to the start of the stat
	r.statLastKey = stat.FirstKey

	// clear out the loaded block (this could be more efficient)
	rows, err = r.s.ReadBlockWithStat(*stat)
	if err != nil {
		fmt.Errorf("error in SegmentReader.ReadBlockWithStat: %w", err)
	}
	r.blockRows = rows
	if r.direction == DirectionDescending {
		slices.Reverse(r.blockRows)
	}

	if (r.direction == DirectionAscending && isUnboundEnd) || (r.direction == DirectionDescending && isUnboundStart) {
		r.blockRowIdx = len(rows)
	} else {
		// Call .Next() until we hit the key or go past it
		for {
			row, err := r.Next()
			if errors.Is(err, io.EOF) {
				// no more, return
				return nil
			}
			if err != nil {
				return fmt.Errorf("error in Next(): %w", err)
			}

			if r.direction == DirectionDescending && bytes.Compare(row.Key, key) <= 0 {
				// We found it or something less than
				break
			}
			if r.direction == DirectionAscending && bytes.Compare(row.Key, key) >= 0 {
				// We found it or something greater than
				break
			}
		}
		// Decrement the block index if we find it
		r.blockRowIdx--
	}

	if isUnboundStart && r.direction == DirectionDescending {
		// special indicator so we don't start from the top
		r.blockRowIdx = -1
	}

	return nil
}

// CloseReader proxies to SegmentReader.Close
func (r *RowIter) CloseReader() error {
	return r.s.Close()
}
