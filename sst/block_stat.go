package sst

type (
	blockStat struct {
		// where in the file this block starts (post compression)
		offset uint64
		// raw size needed for loading into mem (decompression target or direct load)
		rawBytes uint64
		// size of the block after compression, used for decompression
		//
		// 0 if not compressed
		compressedBytes uint64
		// final block bytes (incl compression) hash
		hash uint64
	}
)

func (bs blockStat) toBytes() []byte {
	panic("todo")
}
