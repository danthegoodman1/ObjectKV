package snapshot_reader

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/danthegoodman1/objectkv/sst"
	"testing"
)

func TestGetRow(t *testing.T) {
	// write records across segments
	seg1 := &bytes.Buffer{}
	opts := sst.DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := sst.NewSegmentWriter(
		sst.BytesWriteCloser{
			Buffer: seg1,
		}, opts)

	for i := 0; i < 200; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	segmentLength1, seg1MetaBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}

	seg2 := &bytes.Buffer{}
	w = sst.NewSegmentWriter(
		sst.BytesWriteCloser{
			Buffer: seg2,
		}, opts)

	for i := 1; i < 200; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	segmentLength2, seg2MetaBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Seg3 will be L1, so it should be skipped (duplicate of Seg2)
	seg3 := &bytes.Buffer{}
	w = sst.NewSegmentWriter(
		sst.BytesWriteCloser{
			Buffer: seg3,
		}, opts)

	for i := 1; i < 200; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d-I-SHOULD-NOT-SHOW", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Write something not in the first segment
	key := []byte("key900")
	val := []byte("value900")
	err = w.WriteRow(key, val)
	if err != nil {
		t.Fatal(err)
	}

	segmentLength3, seg3MetaBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// create snapshot reader
	snapReader := NewReader(func(record SegmentRecord) (*sst.SegmentReader, error) {
		var reader sst.SegmentReader
		if record.ID == "1-0" {
			reader = sst.NewSegmentReader(sst.BytesReadSeekCloser{
				Reader: bytes.NewReader(seg1.Bytes()),
			}, int(segmentLength1))
			return &reader, nil
		} else if record.ID == "2-1" {
			reader = sst.NewSegmentReader(sst.BytesReadSeekCloser{
				Reader: bytes.NewReader(seg2.Bytes()),
			}, int(segmentLength2))
			return &reader, nil
		} else if record.ID == "2-0" {
			reader = sst.NewSegmentReader(sst.BytesReadSeekCloser{
				Reader: bytes.NewReader(seg3.Bytes()),
			}, int(segmentLength3))
			return &reader, nil
		}
		panic("unexpected record id: " + record.ID)
	})

	seg1Meta, err := (&sst.SegmentReader{}).BytesToMetadata(seg1MetaBytes)
	if err != nil {
		t.Fatal(err)
	}

	seg2Meta, err := (&sst.SegmentReader{}).BytesToMetadata(seg2MetaBytes)
	if err != nil {
		t.Fatal(err)
	}

	seg3Meta, err := (&sst.SegmentReader{}).BytesToMetadata(seg3MetaBytes)
	if err != nil {
		t.Fatal(err)
	}

	// Add the segments
	snapReader.UpdateSegments([]SegmentRecord{
		{
			ID:       "1-0",
			Level:    0,
			Metadata: *seg1Meta,
		},
		{
			ID:       "2-1",
			Level:    0,
			Metadata: *seg2Meta,
		},
		{
			ID:       "2-0",
			Level:    1,
			Metadata: *seg3Meta,
		},
	}, nil)

	// read row that exists in first segment
	val, err = snapReader.GetRow([]byte("key000"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte("value000"), val) {
		t.Fatal("Got unexpected value:", string(val))
	}

	// read row that exists in another segment
	val, err = snapReader.GetRow([]byte("key001"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte("value001"), val) {
		t.Fatal("Got unexpected value:", string(val))
	}

	// read row that exists in L1 segment
	val, err = snapReader.GetRow([]byte("key900"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte("value900"), val) {
		t.Fatal("Got unexpected value:", string(val))
	}

	// read row that doesn't exist inside the range of the segments
	val, err = snapReader.GetRow([]byte("key999"))
	if !errors.Is(err, sst.ErrNoRows) {
		t.Fatal("unexpected error", err)
	}

	// read row that could exist between items but doesn't
	val, err = snapReader.GetRow([]byte("key800"))
	if !errors.Is(err, sst.ErrNoRows) {
		t.Fatal("unexpected error", err)
	}

	// test dropping the segments and reading again
	snapReader.UpdateSegments(nil, []SegmentRecord{{
		ID:       "2-0",
		Level:    1,
		Metadata: *seg3Meta,
	}})
	val, err = snapReader.GetRow([]byte("key900"))
	if !errors.Is(err, sst.ErrNoRows) {
		t.Fatal("unexpected error", err)
	}
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
