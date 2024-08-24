package snapshot_reader

// NextPossibleKey returns the next possible key forward (asc) or backward (desc) of the current key.
// Accounts for the max length of a possible key
// If an invalid direction is provided then this function is a no-op
func NextPossibleKey(key []byte, direction int) []byte {
	nextKey := make([]byte, 512)
	copy(nextKey[:], key)
	for i := 511; i >= 0; i-- {
		// find the next possible value and incr/decr it
		if nextKey[i] == 0 && direction == DirectionAscending {
			nextKey[i]++
			break
		}
		if nextKey[i] > 0 && direction == DirectionDescending {
			nextKey[i]--
			break
		}
	}
	// Otherwise we do nothing since we don't know the direction
	return nextKey
}
