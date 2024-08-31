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
		noMore      bool
		direction   int
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
	if r.noMore {
		return KVPair{}, io.EOF
	}

	if r.s.closed {
		return KVPair{}, ErrClosed
	}

	if r.blockRows != nil && r.blockRowIdx < len(r.blockRows) {
		// return the row if we have them, and have not reached the end
		pair := r.blockRows[r.blockRowIdx]
		r.blockRowIdx++
		return pair, nil
	}

	// otherwise we need to load the next block's rows
	var stat *BlockStat
	if r.direction == DirectionDescending {
		if r.statLastKey == nil {
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
		r.noMore = true
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

// Seek will seek to the given key, such that any subsequent Next
// call will return that key or after (determined by direction)
func (r *RowIter) Seek(key []byte) error {
	// find the last block first key before this
	var stat *BlockStat
	r.s.metadata.BlockIndex.DescendLessOrEqual(BlockStat{FirstKey: key}, func(item BlockStat) bool {
		stat = &item
		return false
	})

	if stat == nil {
		// there are no more blocks
		r.noMore = true
		return io.EOF
	}

	// Load the block and read until that key or past it
	// todo consider direction

	return nil
}
