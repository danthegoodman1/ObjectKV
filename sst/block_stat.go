package sst

import (
	"bytes"
	"encoding/binary"
)

type (
	blockStat struct {
		firstKey []byte
		// where in the file this block starts (post compression)
		offset uint64
		// final block byte size (incl padding)
		blockSize uint64
		// original size needed for loading into mem (decompression target or direct load)
		originalSize uint64
		// size of the block after compression, used for decompression
		//
		// 0 if not compressed
		compressedSize uint64
		// final block bytes hash (incl compression)
		hash uint64
	}
)

// toBytes returns a byte array according to the spec at SEGMENT.md
func (bs blockStat) toBytes() []byte {
	blockBytes := bytes.Buffer{}

	// add the block's first key info
	blockBytes.Write(binary.LittleEndian.AppendUint16([]byte{}, uint16(len(bs.firstKey))))
	blockBytes.Write(bs.firstKey)

	// write metadata about the data block
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.offset))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.blockSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.originalSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.compressedSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.hash))

	return blockBytes.Bytes()
}
