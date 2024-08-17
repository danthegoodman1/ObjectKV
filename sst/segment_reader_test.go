package sst

import (
	"bytes"
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

	// t.Log("metadata byte hex", hex.EncodeToString(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key000"
	firstValue := "value000"
	secondBlockFirstKey := "key180"
	secondBlockFirstValue := "value180"
	lastKey := "key199"
	lastValue := "value199"

	t.Log(string(metadata.FirstKey), string(metadata.LastKey))
	if string(metadata.FirstKey) != firstKey {
		t.Fatal("first key mismatch")
	}
	if string(metadata.LastKey) != lastKey {
		t.Fatal("last key mismatch")
	}

	metadata.BlockIndex.Ascend(func(item BlockStat) bool {
		t.Log(string(item.FirstKey), fmt.Sprintf("%+v", item))
		return true
	})

	var firstKeyBytes, secondBlockKeyBytes [512]byte
	copy(firstKeyBytes[:], firstKey)
	copy(secondBlockKeyBytes[:], secondBlockFirstKey)

	if metadata.BlockIndex.Len() != 2 {
		t.Fatal("unexpected block index size")
	}

	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); string(item.FirstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.OriginalSize != 3600 {
		t.Fatal("first key block invalid raw bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.CompressedSize != 0 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); int(item.Offset) != 0 {
		t.Fatal("first key block invalid offset")
	}

	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(secondBlockFirstKey)}); string(item.FirstKey) != secondBlockFirstKey {
		t.Fatal("second block invalid first key")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(secondBlockFirstKey)}); item.OriginalSize != 400 {
		t.Fatal("second block invalid raw bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(secondBlockFirstKey)}); item.CompressedSize != 0 {
		t.Fatal("second block invalid compressed bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(secondBlockFirstKey)}); int(item.Offset) != 4096 {
		t.Fatal("second block invalid offset")
	}

	// Read block data
	item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)})
	rows, err := r.ReadBlockWithStat(item)
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
	item, _ = metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(secondBlockFirstKey)})
	secondRows, err := r.ReadBlockWithStat(item)
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
	if !errors.Is(err, ErrNoRows) && err != nil {
		t.Fatal("got something else", row, err)
	}

	row, err = r.GetRow([]byte("key101"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(row.Key, []byte("key101")) {
		t.Fatal("random key bytes not equal, got:", string(row.Key))
	}
	row, err = r.GetRow([]byte("key101"))
	if err != nil {
		t.Fatal(err)
	}
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

	// Read a range
	rows, err = r.GetRange([]byte(firstKey), []byte(secondBlockFirstKey))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 180 {
		t.Fatal("did not get 180 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(firstKey)) {
		t.Fatal("first row did not match first key")
	}
	if !bytes.Equal(rows[0].Value, []byte(firstValue)) {
		t.Fatal("first row did not match first value")
	}
	if !bytes.Equal(rows[len(rows)-1].Key, []byte("key179")) {
		t.Fatal("last row did not match last key", string(rows[len(rows)-1].Key))
	}
	if !bytes.Equal(rows[len(rows)-1].Value, []byte("value179")) {
		t.Fatal("last row did not match last value", string(rows[len(rows)-1].Value))
	}

	// test unbound ranges
	rows, err = r.GetRange([]byte{}, []byte(secondBlockFirstKey))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 180 {
		t.Fatal("did not get 180 rows, got", len(rows))
	}

	rows, err = r.GetRange([]byte(secondBlockFirstKey), []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 20 {
		t.Fatal("did not get 20 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(secondBlockFirstKey)) {
		t.Fatal("first row did not match secondBlockFirstKey")
	}
	if !bytes.Equal(rows[0].Value, []byte(secondBlockFirstValue)) {
		t.Fatal("first row did not match first value")
	}

	if !bytes.Equal(rows[len(rows)-1].Key, []byte("key199")) {
		t.Fatal("last row did not match last key", string(rows[len(rows)-1].Key))
	}
	if !bytes.Equal(rows[len(rows)-1].Value, []byte("value199")) {
		t.Fatal("last row did not match last value", string(rows[len(rows)-1].Value))
	}

	rows, err = r.GetRange([]byte(lastKey), []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatal("did not get 1 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(lastKey)) {
		t.Fatal("first row did not match last key")
	}
	if !bytes.Equal(rows[0].Value, []byte(lastValue)) {
		t.Fatal("first row did not match last value")
	}
}

func TestReadSingleRecordUncompressed(t *testing.T) {
	b := &bytes.Buffer{}
	opts := DefaultSegmentWriterOptions()
	opts.BloomFilter = nil
	w := NewSegmentWriter(b, opts)

	totalBytes := 0
	s := time.Now()
	for i := 0; i < 1; i++ {
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
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key000"
	lastKey := firstKey
	firstValue := "value000"
	lastValue := firstValue

	t.Log(string(metadata.FirstKey), string(metadata.LastKey))
	if string(metadata.FirstKey) != firstKey {
		t.Fatal("first key mismatch")
	}
	if string(metadata.LastKey) != lastKey {
		t.Fatal("last key mismatch")
	}

	metadata.BlockIndex.Ascend(func(item BlockStat) bool {
		t.Log(string(item.FirstKey), fmt.Sprintf("%+v", item))
		return true
	})

	var firstKeyBytes [512]byte
	copy(firstKeyBytes[:], firstKey)

	if metadata.BlockIndex.Len() != 1 {
		t.Fatal("unexpected block index size")
	}

	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); string(item.FirstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.OriginalSize != 20 {
		t.Fatal("first key block invalid raw bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.CompressedSize != 0 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); int(item.Offset) != 0 {
		t.Fatal("first key block invalid offset")
	}

	// Read block data
	item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)})
	rows, err := r.ReadBlockWithStat(item)
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
	if !errors.Is(err, ErrNoRows) && err != nil {
		t.Fatal("got something else", row, err)
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

	// Read a range
	rows, err = r.GetRange([]byte(firstKey), []byte(lastKey))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 0 {
		t.Fatal("did not get 0 rows, got", len(rows))
	}

	rows, err = r.GetRange([]byte{}, []byte(lastKey))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 0 {
		t.Fatal("did not get 0 rows, got", len(rows))
	}

	// test unbound ranges
	rows, err = r.GetRange([]byte{}, []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatal("did not get 1 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(lastKey)) {
		t.Fatal("first row did not match lastKey")
	}
	if !bytes.Equal(rows[0].Value, []byte(lastValue)) {
		t.Fatal("first row did not match last value")
	}
	if !bytes.Equal(rows[0].Key, []byte(firstKey)) {
		t.Fatal("first row did not match firstKey")
	}
	if !bytes.Equal(rows[0].Value, []byte(firstValue)) {
		t.Fatal("first row did not match first value")
	}

	rows, err = r.GetRange([]byte(lastKey), []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatal("did not get 1 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(lastKey)) {
		t.Fatal("first row did not match last key")
	}
	if !bytes.Equal(rows[0].Value, []byte(lastValue)) {
		t.Fatal("first row did not match last value")
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
	t.Log("Wrote", totalBytes, "in", delta, fmt.Sprintf("%.5fMB/s", float64(totalBytes)/1_000_000/delta.Seconds()))

	t.Logf("Got %d metadata bytes", len(metadataBytes))

	// t.Log("metadata byte hex", hex.EncodeToString(metadataBytes))

	// Read the bytes
	r := NewSegmentReader(bytes.NewReader(b.Bytes()), int(segmentLength), DefaultSegmentReaderOptions())
	metadata, err := r.BytesToMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstKey := "key000"
	firstValue := "value000"
	lastKey := "key199"
	lastValue := "value199"

	t.Log(string(metadata.FirstKey), string(metadata.LastKey))
	if string(metadata.FirstKey) != firstKey {
		t.Fatal("first key mismatch")
	}
	if string(metadata.LastKey) != lastKey {
		t.Fatal("last key mismatch")
	}

	metadata.BlockIndex.Ascend(func(item BlockStat) bool {
		t.Log(string(item.FirstKey), fmt.Sprintf("%+v", item))
		return true
	})

	if metadata.BlockIndex.Len() != 1 {
		t.Fatal("unexpected block index size")
	}

	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); string(item.FirstKey) != firstKey {
		t.Fatal("first block invalid first key")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.OriginalSize != 4000 {
		t.Fatal("first key block invalid raw bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.CompressedSize != 298 {
		// if metadata.BlockIndex[firstKeyBytes].compressedSize != 29 {
		t.Fatal("first key block invalid compressed bytes")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); int(item.Offset) != 0 {
		t.Fatal("first key block invalid offset")
	}
	if item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)}); item.Hash != 7503979350938866005 {
		t.Fatal("first key block hash invalid")
	}

	// Read block data
	item, _ := metadata.BlockIndex.Get(BlockStat{FirstKey: []byte(firstKey)})
	rows, err := r.ReadBlockWithStat(item)
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
	if !errors.Is(err, ErrNoRows) && err != nil {
		t.Fatal("got something else", row, err)
	}

	row, err = r.GetRow([]byte("key101"))
	if err != nil {
		t.Fatal(err)
	}
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

	// Read a range
	rows, err = r.GetRange([]byte(firstKey), []byte(lastKey))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 199 {
		t.Fatal("did not get 199 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(firstKey)) {
		t.Fatal("first row did not match first key")
	}
	if !bytes.Equal(rows[0].Value, []byte(firstValue)) {
		t.Fatal("first row did not match first value")
	}
	if !bytes.Equal(rows[len(rows)-1].Key, []byte("key198")) {
		t.Fatal("last row did not match last key", string(rows[len(rows)-1].Key))
	}
	if !bytes.Equal(rows[len(rows)-1].Value, []byte("value198")) {
		t.Fatal("last row did not match last value", string(rows[len(rows)-1].Value))
	}

	rows, err = r.GetRange([]byte("key180"), []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 20 {
		t.Fatal("did not get 20 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte("key180")) {
		t.Fatal("first row did not match secondBlockFirstKey")
	}
	if !bytes.Equal(rows[0].Value, []byte("value180")) {
		t.Fatal("first row did not match first value")
	}

	if !bytes.Equal(rows[len(rows)-1].Key, []byte("key199")) {
		t.Fatal("last row did not match last key", string(rows[len(rows)-1].Key))
	}
	if !bytes.Equal(rows[len(rows)-1].Value, []byte("value199")) {
		t.Fatal("last row did not match last value", string(rows[len(rows)-1].Value))
	}

	rows, err = r.GetRange([]byte(lastKey), []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatal("did not get 1 rows, got", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte(lastKey)) {
		t.Fatal("first row did not match last key")
	}
	if !bytes.Equal(rows[0].Value, []byte(lastValue)) {
		t.Fatal("first row did not match last value")
	}
}

// todo test probe bloom filter
