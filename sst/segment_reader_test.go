package sst

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestReadUncompressed(t *testing.T) {
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
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key0"
	firstValue := "value0"
	secondBlockFirstKey := "key191"
	secondBlockFirstValue := "value191"
	lastKey := "key199"
	lastValue := "value199"

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

	if len(metadata.blockIndex) != 2 {
		t.Fatal("unexpected block index size")
	}

	if string(metadata.blockIndex[firstKeyBytes].firstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if metadata.blockIndex[firstKeyBytes].originalSize != 3600 {
		t.Fatal("first key block invalid raw bytes")
	}
	if metadata.blockIndex[firstKeyBytes].compressedSize != 0 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if int(metadata.blockIndex[firstKeyBytes].offset) != 0 {
		t.Fatal("first key block invalid offset")
	}

	if string(metadata.blockIndex[secondBlockKeyBytes].firstKey) != secondBlockFirstKey {
		t.Fatal("second block invalid first key")
	}
	if metadata.blockIndex[secondBlockKeyBytes].originalSize != 180 {
		t.Fatal("second block invalid raw bytes")
	}
	if metadata.blockIndex[secondBlockKeyBytes].compressedSize != 0 {
		t.Fatal("second block invalid compressed bytes")
	}
	if int(metadata.blockIndex[secondBlockKeyBytes].offset) != 4096 {
		t.Fatal("second block invalid offset")
	}

	// Read block data
	rows, err := r.readBlockWithStartKey([]byte(firstKey))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(rows), "rows")

	if val, exists := rows[firstKeyBytes]; !exists {
		t.Fatal("did not exist")
	} else if string(val) != firstValue {
		t.Fatal("value didn't match")
	}

	// read the second block
	secondRows, err := r.readBlockWithStartKey([]byte(secondBlockFirstKey))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(secondRows), "rows")
	if len(secondRows)+len(rows) != 200 {
		t.Fatal("did not get 200 rows, got", len(secondRows)+len(rows))
	}

	if val, exists := secondRows[secondBlockKeyBytes]; !exists {
		t.Fatal("did not exist")
	} else if string(val) != secondBlockFirstValue {
		t.Fatal("value didn't match")
	}

	var lastKeyBytes [512]byte
	copy(lastKeyBytes[:], lastKey)
	if val, exists := secondRows[lastKeyBytes]; !exists {
		t.Fatal("did not exist")
	} else if string(val) != lastValue {
		t.Fatal("value didn't match")
	}
}

func TestReadCompressionZSTD(t *testing.T) {
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
	segmentLength, metadataBytes, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(s)
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.5fMB/s", float64(totalBytes)/1_000_000/delta.Seconds()))

	t.Logf("Got %d metadata bytes", len(metadataBytes))

	t.Log("metadata byte hex", hex.EncodeToString(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key0"
	firstValue := "value0"
	lastKey := "key199"
	lastValue := "value199"

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

	var firstKeyBytes [512]byte
	copy(firstKeyBytes[:], firstKey)

	if len(metadata.blockIndex) != 1 {
		t.Fatal("unexpected block index size")
	}

	if string(metadata.blockIndex[firstKeyBytes].firstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if metadata.blockIndex[firstKeyBytes].originalSize != 3780 {
		t.Fatal("first key block invalid raw bytes")
	}
	if metadata.blockIndex[firstKeyBytes].compressedSize != 436 {
		// if metadata.blockIndex[firstKeyBytes].compressedSize != 29 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if int(metadata.blockIndex[firstKeyBytes].offset) != 0 {
		t.Fatal("first key block invalid offset")
	}
	if metadata.blockIndex[firstKeyBytes].hash != 4760777162451107343 {
		// if metadata.blockIndex[firstKeyBytes].hash != 2324848862588043792 {
		t.Fatal("first key block hash invalid")
	}

	// Read block data
	rows, err := r.readBlockWithStartKey([]byte(firstKey))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(rows), "rows")
	if len(rows) != 200 {
		t.Fatal("did not get 200 rows, got", len(rows))
	}

	if val, exists := rows[firstKeyBytes]; !exists {
		t.Fatal("did not exist")
	} else if string(val) != firstValue {
		t.Fatal("value didn't match")
	}

	var lastKeyBytes [512]byte
	copy(lastKeyBytes[:], lastKey)
	if val, exists := rows[lastKeyBytes]; !exists {
		t.Fatal("did not exist")
	} else if string(val) != lastValue {
		t.Fatal("value didn't match")
	}
}

// todo test probe bloom filter
