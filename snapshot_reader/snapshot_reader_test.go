package snapshot_reader

import (
	"bytes"
	"github.com/danthegoodman1/objectkv/sst"
	"testing"
)

func TestGetRow(t *testing.T) {
	// todo write record
	// todo create snapshot reader
	// todo read row that exists in first segment
	// todo read row that exists in another segment
	// todo read row that doesn't exist outside the range of the segments
	// todo read row that could exist between items but doesn't
}

func TestGetRange(t *testing.T) {
	// todo write records
	// todo create snapshot reader
	// todo get a range of rows that exist
	//   ensure they are in the right order
	// todo get a range of rows that would only have 1 in middle, ensure only 1
	// todo get a range of rows that would only have 1 from start, ensure first key
	// todo get an empty range
	// todo get a range in the desc order, check right order
	// todo get a single row in desc order, ensure top key
	// todo ensure can get unlimited range
}

func TestFindMaxIndexes(t *testing.T) {
	items := []sst.KVPair{
		{
			Key: []byte("b"),
		},
		{
			Key: []byte("b"),
		},
		{
			Key: []byte("a"),
		},
		{
			Key: []byte("b"),
		},
	}

	indexes := findMaxIndexes(items, func(a, b sst.KVPair) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// verify result is []int{0, 1, 3}
	expected := []int{0, 1, 3}

	if len(indexes) != len(expected) {
		t.Errorf("Expected %d indexes, but got %d", len(expected), len(indexes))
	}

	for i, v := range expected {
		if i >= len(indexes) || indexes[i] != v {
			t.Errorf("Mismatch at position %d: expected %d, got %d", i, v, indexes[i])
		}
	}
}
