package sst

import (
	"bytes"
	"encoding/hex"
	"errors"
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

	metadata.blockIndex.Ascend(func(item blockStat) bool {
		t.Log(string(item.firstKey), fmt.Sprintf("%+v", item))
		return true
	})

	var firstKeyBytes, secondBlockKeyBytes [512]byte
	copy(firstKeyBytes[:], firstKey)
	copy(secondBlockKeyBytes[:], secondBlockFirstKey)

	if metadata.blockIndex.Len() != 2 {
		t.Fatal("unexpected block index size")
	}

	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); string(item.firstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); item.originalSize != 3600 {
		t.Fatal("first key block invalid raw bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); item.compressedSize != 0 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); int(item.offset) != 0 {
		t.Fatal("first key block invalid offset")
	}

	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(secondBlockFirstKey)}); string(item.firstKey) != secondBlockFirstKey {
		t.Fatal("second block invalid first key")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(secondBlockFirstKey)}); item.originalSize != 180 {
		t.Fatal("second block invalid raw bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(secondBlockFirstKey)}); item.compressedSize != 0 {
		t.Fatal("second block invalid compressed bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(secondBlockFirstKey)}); int(item.offset) != 4096 {
		t.Fatal("second block invalid offset")
	}

	// Read block data
	item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)})
	rows, err := r.readBlockWithStat(item)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(rows), "rows")

	if string(rows[0].Key) != firstKey {
		t.Fatal("first key didn't match")
	}
	if string(rows[0].Value) != firstValue {
		t.Fatal("first value didn't match")
	}

	// read the second block
	item, _ = metadata.blockIndex.Get(blockStat{firstKey: []byte(secondBlockFirstKey)})
	secondRows, err := r.readBlockWithStat(item)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(secondRows), "rows")
	if len(secondRows)+len(rows) != 200 {
		t.Fatal("did not get 200 rows, got", len(secondRows)+len(rows))
	}

	if string(secondRows[0].Key) != secondBlockFirstKey {
		t.Fatal("second block first key didn't match")
	}
	if string(secondRows[0].Value) != secondBlockFirstValue {
		t.Fatal("second block first value didn't match")
	}

	if string(secondRows[len(secondRows)-1].Key) != lastKey {
		t.Fatal("last key didn't match")
	}
	if string(secondRows[len(secondRows)-1].Value) != lastValue {
		t.Fatal("last value didn't match")
	}

	// read some rows
	row, err := r.GetRow([]byte(firstKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte(firstKey)) {
		t.Fatal("first key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte(firstValue)) {
		t.Fatal("first value bytes not equal")
	}

	row, err = r.GetRow([]byte("fuhguiregui"))
	if !errors.Is(err, ErrNoRows) {
		t.Fatal("got something else", row, err)
	}

	row, err = r.GetRow([]byte("key101"))
	if !bytes.Equal(row.Key, []byte("key101")) {
		t.Fatal("random key bytes not equal")
	}
	row, err = r.GetRow([]byte("key101"))
	if !bytes.Equal(row.Value, []byte("value101")) {
		t.Fatal("random value bytes not equal")
	}

	row, err = r.GetRow([]byte(lastKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte(lastKey)) {
		t.Fatal("last key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte(lastValue)) {
		t.Fatal("last value bytes not equal")
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

	metadata.blockIndex.Ascend(func(item blockStat) bool {
		t.Log(string(item.firstKey), fmt.Sprintf("%+v", item))
		return true
	})

	if metadata.blockIndex.Len() != 1 {
		t.Fatal("unexpected block index size")
	}

	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); string(item.firstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); item.originalSize != 3780 {
		t.Fatal("first key block invalid raw bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); item.compressedSize != 436 {
		// if metadata.blockIndex[firstKeyBytes].compressedSize != 29 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); int(item.offset) != 0 {
		t.Fatal("first key block invalid offset")
	}
	if item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)}); item.hash != 4760777162451107343 {
		// if metadata.blockIndex[firstKeyBytes].hash != 2324848862588043792 {
		t.Fatal("first key block hash invalid")
	}

	// Read block data
	item, _ := metadata.blockIndex.Get(blockStat{firstKey: []byte(firstKey)})
	rows, err := r.readBlockWithStat(item)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Read", len(rows), "rows")
	if len(rows) != 200 {
		t.Fatal("did not get 200 rows, got", len(rows))
	}

	if string(rows[0].Key) != firstKey {
		t.Fatal("first key didn't match")
	}
	if string(rows[0].Value) != firstValue {
		t.Fatal("last value didn't match")
	}

	if string(rows[len(rows)-1].Key) != lastKey {
		t.Fatal("last key didn't match")
	}
	if string(rows[len(rows)-1].Value) != lastValue {
		t.Fatal("last value didn't match")
	}

	// read some rows
	row, err := r.GetRow([]byte(firstKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte(firstKey)) {
		t.Fatal("first key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte(firstValue)) {
		t.Fatal("first value bytes not equal")
	}

	row, err = r.GetRow([]byte("fuhguiregui"))
	if !errors.Is(err, ErrNoRows) {
		t.Fatal("got something else", row, err)
	}

	row, err = r.GetRow([]byte("key101"))
	if !bytes.Equal(row.Key, []byte("key101")) {
		t.Fatal("random key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte("value101")) {
		t.Fatal("random value bytes not equal")
	}

	row, err = r.GetRow([]byte(lastKey))
	if !bytes.Equal(row.Key, []byte(lastKey)) {
		t.Fatal("last key bytes not equal")
	}
	if !bytes.Equal(row.Value, []byte(lastValue)) {
		t.Fatal("last value bytes not equal")
	}
}

// todo test probe bloom filter
