package sst

import (
	"bytes"
	"fmt"
	"io"
)

type (
	RowIter struct {
		reader      io.ReadSeeker
		statLastKey []byte
		blockRows   []KVPair
		blockRowIdx int
		s           *SegmentReader
	}
)

// Next returns io.EOF when there are no more rows.
func (r *RowIter) Next() (KVPair, error) {
	if r.blockRows != nil && r.blockRowIdx < len(r.blockRows) {
		// return the row if we have them, and have not reached the end
		pair := r.blockRows[r.blockRowIdx]
		r.blockRowIdx++
		return pair, nil
	}

	// otherwise we need to load the next block's rows
	var stat BlockStat
	r.s.metadata.BlockIndex.AscendGreaterOrEqual(BlockStat{FirstKey: r.statLastKey}, func(item BlockStat) bool {
		if bytes.Equal(r.statLastKey, item.FirstKey) {
			// keep going, this is the same key
			return true
		}

		// Otherwise we take it and exit (next stat)
		r.statLastKey = item.FirstKey
		stat = item
		return false
	})
	rows, err := r.s.readBlockWithStat(stat)
	if err != nil {
		return KVPair{}, fmt.Errorf("error in SegmentReader.readBlockWithStat: %w", err)
	}

	r.blockRows = rows

	r.blockRowIdx = 1
	return r.blockRows[0], nil
}
