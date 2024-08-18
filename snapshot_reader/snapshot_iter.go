package snapshot_reader

import "github.com/danthegoodman1/objectkv/sst"

type (
	Iter struct {
		reader *Reader
	}
)

// Next provides the next value, progressing the interator
func (i *Iter) Next() (sst.KVPair, error) {
	panic("todo")
}

// Peek provides the next value without progressing the iterator
func (i *Iter) Peek() (sst.KVPair, error) {
	panic("todo")
}

func (i *Iter) prepareSegmentIters() error {
	panic("todo")
}