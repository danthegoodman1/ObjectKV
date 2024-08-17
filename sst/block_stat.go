package sst

import (
	"bytes"
	"encoding/binary"
)

type (
	BlockStat struct {
		FirstKey []byte
		// where in the file this block starts (post compression)
		Offset uint64
		// final block byte size (incl padding)
		BlockSize uint64
		// original size needed for loading into mem (decompression target or direct load)
		OriginalSize uint64
		// size of the block after compression, used for decompression
		//
		// 0 if not compressed
		CompressedSize uint64
		// final block bytes hash (incl compression)
		Hash uint64
	}
)

// toBytes returns a byte array according to the spec at SEGMENT.md
func (bs BlockStat) toBytes() []byte {
	blockBytes := bytes.Buffer{}

	// add the block's first key info
	blockBytes.Write(binary.LittleEndian.AppendUint16([]byte{}, uint16(len(bs.FirstKey))))
	blockBytes.Write(bs.FirstKey)

	// write metadata about the data block
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.Offset))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.BlockSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.OriginalSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.CompressedSize))
	blockBytes.Write(binary.LittleEndian.AppendUint64([]byte{}, bs.Hash))

	return blockBytes.Bytes()
}
