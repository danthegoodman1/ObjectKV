package tuple

// From the FoundationDB spec https://github.com/apple/foundationdb/blob/main/design/tuple.md

import (
	"errors"
	"strings"
)

const (
	itemSeparator = "/"
	nullByte      = "\x00"
	boundaryByte  = "\xFF"

	// Type codes from FDB spec
	typeNull   = "\x00"
	typeBytes  = "\x01"
	typeString = "\x02"
	typeNested = "\x05"
)

// Tuple represents an ordered list of elements that can be encoded into a key. Tuples can be nested.
type Tuple []any

// Encode encodes a sequence of items into a sortable byte key.
// Each item can be a string, []byte, nil, or Tuple (nested tuple)
func (t Tuple) Encode() []byte {
	var result []byte
	result = append(result, itemSeparator[0])

	for _, item := range t {
		switch v := item.(type) {
		case nil:
			// Null is encoded as \x00\xFF
			result = append(result, nullByte[0], boundaryByte[0])
		case string:
			// Unicode strings are encoded as \x02 + UTF8 bytes with null escaped + \x00
			result = append(result, typeString[0])
			result = append(result, escapeNulls([]byte(v))...)
			result = append(result, nullByte[0])
		case []byte:
			// Byte strings are encoded as \x01 + bytes with null escaped + \x00
			result = append(result, typeBytes[0])
			result = append(result, escapeNulls(v)...)
			result = append(result, nullByte[0])
		case Tuple:
			// Nested tuples are encoded as \x05 + encoded elements + \x00
			result = append(result, typeNested[0])
			encoded := v.Encode()
			// Skip the leading separator when nesting
			if len(encoded) > 1 {
				result = append(result, encoded[1:]...)
			}
			result = append(result, nullByte[0])
		}
	}

	return result
}

// escapeNulls replaces null bytes with \x00\xFF to maintain proper ordering
func escapeNulls(b []byte) []byte {
	result := make([]byte, 0, len(b)*2)
	for _, c := range b {
		if c == nullByte[0] {
			result = append(result, nullByte[0], boundaryByte[0])
		} else {
			result = append(result, c)
		}
	}
	return result
}

// GetPrefixRange returns (start_key, end_key) for range query to list all tuples
// that match the given prefix items.
func (t Tuple) GetPrefixRange() ([]byte, []byte) {
	startKey := t.Encode()
	endKey := append(startKey, boundaryByte[0])
	return startKey, endKey
}

// Pack creates a new Tuple from the provided items.
// Items can be strings, []bytes, nil, or Tuples (nested tuples)
//
// Example:
//
//	Pack("hello", []byte{1, 2, 3}, nil, Pack("nested", "tuple"))
//
// The tuple can be encoded into a key with Tuple.Encode()
//
// They can subsequently be decoded with Decode()
//
//	decoded, err := Decode(encoded)
func Pack(items ...any) Tuple {
	// Return a copy of the items as a helper for the caller
	return items
}

// ErrInvalidTuple indicates the encoded tuple string is malformed
var ErrInvalidTuple = errors.New("invalid tuple encoding")

// Decode parses an encoded tuple byte slice back into a Tuple.
// Returns error if the encoding is invalid.
func Decode(encoded []byte) (Tuple, error) {
	if len(encoded) == 0 || encoded[0] != itemSeparator[0] {
		return nil, ErrInvalidTuple
	}

	var result Tuple
	pos := 1 // Skip initial separator

	for pos < len(encoded) {
		if pos >= len(encoded) {
			return nil, ErrInvalidTuple
		}

		switch encoded[pos] {
		case nullByte[0]:
			if pos+1 >= len(encoded) || encoded[pos+1] != boundaryByte[0] {
				return nil, ErrInvalidTuple
			}
			result = append(result, nil)
			pos += 2

		case typeString[0]:
			val, newPos, err := decodeString(encoded[pos+1:])
			if err != nil {
				return nil, err
			}
			result = append(result, val)
			pos += 1 + newPos

		case typeBytes[0]:
			val, newPos, err := decodeBytes(encoded[pos+1:])
			if err != nil {
				return nil, err
			}
			result = append(result, val)
			pos += 1 + newPos

		case typeNested[0]:
			val, newPos, err := decodeNested(encoded[pos+1:])
			if err != nil {
				return nil, err
			}
			result = append(result, val)
			pos += 1 + newPos

		default:
			return nil, ErrInvalidTuple
		}
	}

	return result, nil
}

// decodeString reads a string value until null terminator, handling escaped nulls
func decodeString(encoded []byte) (string, int, error) {
	var result strings.Builder
	pos := 0

	for pos < len(encoded) {
		// Check for terminator
		if encoded[pos] == nullByte[0] {
			if pos+1 < len(encoded) && encoded[pos+1] == boundaryByte[0] {
				// This is an escaped null
				result.WriteByte(nullByte[0])
				pos += 2
				continue
			}
			// This is the terminator
			return result.String(), pos + 1, nil
		}

		result.WriteByte(encoded[pos])
		pos++
	}

	return "", 0, ErrInvalidTuple
}

// decodeBytes reads a byte array value until null terminator, handling escaped nulls
func decodeBytes(encoded []byte) ([]byte, int, error) {
	var result []byte
	pos := 0

	for pos < len(encoded) {
		// Check for terminator
		if encoded[pos] == nullByte[0] {
			if pos+1 < len(encoded) && encoded[pos+1] == boundaryByte[0] {
				// This is an escaped null
				result = append(result, nullByte[0])
				pos += 2
				continue
			}
			// This is the terminator
			return result, pos + 1, nil
		}

		result = append(result, encoded[pos])
		pos++
	}

	return nil, 0, ErrInvalidTuple
}

// decodeNested reads a nested tuple until null terminator
func decodeNested(encoded []byte) (Tuple, int, error) {
	var result Tuple
	pos := 0

	for pos < len(encoded) {
		// Check for terminator of the nested tuple
		if encoded[pos] == nullByte[0] {
			// Look ahead to see if this is an escaped null
			if pos+1 < len(encoded) && encoded[pos+1] == boundaryByte[0] {
				// This is a null value, not a terminator
				result = append(result, nil)
				pos += 2
				continue
			}
			// This is the terminator
			return result, pos + 1, nil
		}

		switch encoded[pos] {
		case typeString[0]:
			val, newPos, err := decodeString(encoded[pos+1:])
			if err != nil {
				return nil, 0, err
			}
			result = append(result, val)
			pos += 1 + newPos

		case typeBytes[0]:
			val, newPos, err := decodeBytes(encoded[pos+1:])
			if err != nil {
				return nil, 0, err
			}
			result = append(result, val)
			pos += 1 + newPos

		case typeNested[0]:
			innerTuple, newPos, err := decodeNested(encoded[pos+1:])
			if err != nil {
				return nil, 0, err
			}
			result = append(result, innerTuple)
			pos += 1 + newPos

		default:
			return nil, 0, ErrInvalidTuple
		}
	}

	return nil, 0, ErrInvalidTuple
}
