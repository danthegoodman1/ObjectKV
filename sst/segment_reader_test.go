package sst

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestReadReturnedMetadataUncompressed(t *testing.T) {
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
	segmentLength, metadataBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.2fMB/s", float64(totalBytes)/1_000_000/delta.Seconds())) // 22MB/s

	t.Logf("Got %d metadata bytes", len(metadataBytes))

	t.Log("metadata byte hex", hex.EncodeToString(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(nil, int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key0"
	secondBlockFirstKey := "key191"
	lastKey := "key199"

	t.Log(string(metadata.firstKey), string(metadata.lastKey))
	if string(metadata.firstKey) != firstKey {
		t.Fatal("first key mismatch")
	}
	if string(metadata.lastKey) != lastKey {
		t.Fatal("last key mismatch")
	}

	for key, stat := range metadata.blockIndex {
		t.Log(string(key[:]), fmt.Sprintf("%+v", stat))
	}

	var firstKeyBytes, secondBlockKeyBytes [512]byte
	copy(firstKeyBytes[:], firstKey)
	copy(secondBlockKeyBytes[:], secondBlockFirstKey)

	if string(metadata.blockIndex[firstKeyBytes].firstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if metadata.blockIndex[firstKeyBytes].rawBytes != 3600 {
		t.Fatal("first key block invalid raw bytes")
	}
	if metadata.blockIndex[firstKeyBytes].compressedBytes != 0 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if int(metadata.blockIndex[firstKeyBytes].offset) != 0 {
		t.Fatal("first key block invalid offset")
	}

	if string(metadata.blockIndex[secondBlockKeyBytes].firstKey) != secondBlockFirstKey {
		t.Fatal("second block invalid first key")
	}
	if metadata.blockIndex[secondBlockKeyBytes].rawBytes != 180 {
		t.Fatal("second block invalid raw bytes")
	}
	if metadata.blockIndex[secondBlockKeyBytes].compressedBytes != 0 {
		t.Fatal("second block invalid compressed bytes")
	}
	if int(metadata.blockIndex[secondBlockKeyBytes].offset) != 4096 {
		t.Fatal("second block invalid offset")
	}
}
