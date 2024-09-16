package sst

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestRowIterNext(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := NewSegmentWriter(
		BytesWriteCloser{
			Buffer: b,
		}, opts)

	totalBytes := 0
	s := time.Now()
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
		totalBytes += len(key) + len(val)
	}
	segmentLength, metadataBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.2fMB/s", float64(totalBytes)/1_000_000/delta.Seconds())) // 22MB/s

	t.Logf("Got %d metadata bytes", len(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(
		BytesReadSeekCloser{
			Reader: bytes.NewReader(b.Bytes()),
		}, int(segmentLength))
	iter, err := r.RowIter(DirectionAscending)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Close()

	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte("key000")) {
		t.Fatal("first row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value000")) {
		t.Fatal("first row value bytes not equal")
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte("key001")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value001")) {
		t.Fatal("second row value bytes not equal")
	}

	for range 198 {
		row, err = iter.Next()
		if err != nil {
			t.Fatal(err)
		}
	}

	if !bytes.Equal(row.Key, []byte("key199")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value199")) {
		t.Fatal("second row value bytes not equal")
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal("got unexpected error value", err)
	}

	// Descending iter
	iter, err = r.RowIter(DirectionDescending)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte("key199")) {
		t.Fatal("first row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value199")) {
		t.Fatal("first row value bytes not equal")
	}
	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte("key198")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value198")) {
		t.Fatal("second row value bytes not equal")
	}

	for range 197 {
		row, err = iter.Next()
		if err != nil {
			t.Fatal(err)
		}
	}

	if !bytes.Equal(row.Key, []byte("key001")) {
		t.Fatal("final row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value001")) {
		t.Fatal("final row value bytes not equal")
	}
}

func TestRowIterSeek(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := NewSegmentWriter(
		BytesWriteCloser{
			Buffer: b,
		}, opts)

	totalBytes := 0
	s := time.Now()
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
		totalBytes += len(key) + len(val)
	}
	segmentLength, metadataBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.2fMB/s", float64(totalBytes)/1_000_000/delta.Seconds())) // 22MB/s

	t.Logf("Got %d metadata bytes", len(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(
		BytesReadSeekCloser{
			Reader: bytes.NewReader(b.Bytes()),
		}, int(segmentLength))
	defer r.Close()

	iter, err := r.RowIter(DirectionAscending)
	if err != nil {
		t.Fatal(err)
	}

	err = iter.Seek([]byte("key010"))
	if err != nil {
		t.Fatal(err)
	}

	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key010")) {
		t.Fatal("first row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value010")) {
		t.Fatal("first row value bytes not equal")
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key011")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value011")) {
		t.Fatal("second row value bytes not equal")
	}

	// seek to the beginning
	err = iter.Seek(UnboundStart)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key000")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value000")) {
		t.Fatal("second row value bytes not equal")
	}

	// seek out of range
	err = iter.Seek([]byte("key200"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	// Seek to unbound end
	err = iter.Seek(UnboundEnd)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	// check row iter descending
	iter, err = r.RowIter(DirectionDescending)
	if err != nil {
		t.Fatal(err)
	}

	err = iter.Seek([]byte("key010"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key010")) {
		t.Fatal("first row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value010")) {
		t.Fatal("first row value bytes not equal")
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key009")) {
		t.Fatal("second row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value009")) {
		t.Fatal("second row value bytes not equal")
	}

	// seek to the beginning
	err = iter.Seek(UnboundStart)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err, string(row.Key), iter.blockRowIdx)
	}

	err = iter.Seek(UnboundEnd)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key199")) {
		t.Fatal("next row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value199")) {
		t.Fatal("next row value bytes not equal")
	}

	// seek out of range
	err = iter.Seek([]byte("key200"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key199")) {
		t.Fatal("next row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value199")) {
		t.Fatal("next row value bytes not equal")
	}

	// seek before range
	err = iter.Seek([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	// Seek to unbound end
	err = iter.Seek(UnboundEnd)
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key199")) {
		t.Fatal("next row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value199")) {
		t.Fatal("next row value bytes not equal")
	}

	// Seek to end and check we EOF
	err = iter.Seek([]byte("key000"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(row.Key, []byte("key000")) {
		t.Fatal("next row key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value000")) {
		t.Fatal("next row value bytes not equal")
	}

	row, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestRollover(t *testing.T) {
	opts := DefaultSegmentWriterOptions()
	seg := &bytes.Buffer{}
	w := NewSegmentWriter(
		BytesWriteCloser{
			Buffer: seg,
		}, opts)

	for i := 1; i < 200; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("value%03d-I-SHOULD-NOT-SHOW", i)) // todo removing the i should not show changes the wrap high value
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Write something not in the first segment
	key := []byte("key900")
	val := []byte("value900")
	err := w.WriteRow(key, val)
	if err != nil {
		t.Fatal(err)
	}

	segmentLength3, _, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}

	r := NewSegmentReader(
		BytesReadSeekCloser{
			Reader: bytes.NewReader(seg.Bytes()),
		}, int(segmentLength3))
	defer r.Close()

	iter, err := r.RowIter(DirectionDescending)
	if err != nil {
		t.Fatal(err)
	}

	err = iter.Seek([]byte("key006"))
	if err != nil {
		t.Fatal(err)
	}

	pair, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(pair.Key))

	pair, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(pair.Key))
	pair, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(pair.Key))

	pair, err = iter.Next()
	if !errors.Is(err, io.EOF) {
		t.Log("expected nothing, got", string(pair.Key))
		t.Fatal(err)
	}
}
