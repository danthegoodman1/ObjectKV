package snapshot_reader

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/danthegoodman1/objectkv/sst"
	"io"
)

type (
	Iter struct {
		reader    *Reader
		direction int
		lastKey   []byte
		rowBuffer *list.List
		options   iterOptions
		done      bool
	}

	iterOptions struct {
		bufferSize int
	}

	IterOption func(options *iterOptions)
)

var (
	defaultIterOptions = iterOptions{
		bufferSize: 100,
	}
)

// Next provides the next value, progressing the iterator.
// Returns io.EOF if there are no more rows
func (i *Iter) Next() (sst.KVPair, error) {
	if err := i.checkLoadBuffer(); err != nil {
		return sst.KVPair{}, err
	}

	// pop the first item in the list and return it
	elem := i.rowBuffer.Front()
	kvPair := i.rowBuffer.Remove(elem).(sst.KVPair)

	return kvPair, nil
}

// Peek provides the next value without progressing the iterator.
// Returns io.EOF if there are no more rows
func (i *Iter) Peek() (sst.KVPair, error) {
	if err := i.checkLoadBuffer(); err != nil {
		return sst.KVPair{}, err
	}

	// read the first item in the list and return it
	kvPair := i.rowBuffer.Front().Value.(sst.KVPair)

	return kvPair, nil
}

// checkLoadBuffer will check if we have an empty buffer, and load it.
// If the end has been reached, it will return an io.EOF
func (i *Iter) checkLoadBuffer() error {
	if i.rowBuffer.Len() > 0 {
		// We have data, no need to do anything
		return nil
	}

	if i.done {
		// we hit the end earlier with this snapshot, nothing will change
		return io.EOF
	}

	// figure out what our keys are based on direction
	var startKey, endKey []byte
	if i.direction == sst.DirectionDescending {
		startKey = sst.UnboundStart
		endKey = i.lastKey
	} else {
		// default ascending
		startKey = i.lastKey
		endKey = sst.UnboundEnd
	}

	// load the range
	rows, err := i.reader.GetRange(startKey, endKey, i.options.bufferSize, i.direction)
	if err != nil {
		return fmt.Errorf("error in Reader.GetRange: %w", err)
	}
	if len(rows) == 0 {
		i.done = true
		return io.EOF
	}

	// add the rows to the linked list
	i.rowBuffer = list.New()
	for ind, row := range rows {
		if ind == 0 && bytes.Equal(row.Key, i.lastKey) {
			// Because the range is [start, end), we will get the `end` key again
			// as the next page first key (start) if we are ascending
			continue
		}

		i.rowBuffer.PushBack(row)
	}

	// Set the last key
	i.lastKey = i.rowBuffer.Back().Value.(sst.KVPair).Key
	return nil
}

func RowBufferSize(size int) IterOption {
	return func(options *iterOptions) {
		options.bufferSize = size
	}
}
