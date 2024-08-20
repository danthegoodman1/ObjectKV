package snapshot_reader

// NextPossibleKey returns the next possible key asc (0) or desc (1)
func NextPossibleKey(key []byte, direction int) []byte {
	nextKey := make([]byte, 512)
	copy(nextKey[:], key)
	if direction == DirectionForward {
		nextKey[511] = nextKey[511] << 1
	} else if direction == DirectionForward {
		nextKey[511] = nextKey[511] >> 1
	}
	// Otherwise we do nothing since we don't know the direction
	return nextKey
}
