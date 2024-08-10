package sst

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestSegmentWriterNoCompression(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := NewSegmentWriter(b, opts)

	totalBytes := 0
	s := time.Now()
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
		totalBytes += len(key) + len(val)
	}
	segmentLen, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.2fMB/s", float64(totalBytes)/1_000_000/delta.Seconds())) // 22MB/s
	t.Log(hex.EncodeToString(b.Bytes()))
	t.Log("Got segment length", segmentLen)
}

func TestSegmentWriterZSTD(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	opts.ZSTDCompressionLevel = 1
	w := NewSegmentWriter(b, opts)

	totalBytes := 0
	s := time.Now()
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		err := w.WriteRow(key, val)
		if err != nil {
			t.Fatal(err)
		}
		totalBytes += len(key) + len(val)
	}
	segmentLen, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.5fMB/s", float64(totalBytes)/1_000_000/delta.Seconds()))
	// t.Log(hex.EncodeToString(b.Bytes()))
	t.Log("Got segment length", segmentLen)
}
