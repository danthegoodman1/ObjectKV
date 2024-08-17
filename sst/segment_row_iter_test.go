package sst

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestRowIter(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := NewSegmentWriter(b, opts)

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

	t.Log("metadata byte hex", hex.EncodeToString(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	iter, err := r.RowIter()
	if err != nil {
		t.Fatal(err)
	}

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
}
