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

// EncodeTuple encodes a sequence of items into a sortable string key.
// Each item can be a string, []byte, nil, or Tuple (nested tuple)
func (t Tuple) Encode() string {
	var result strings.Builder
	result.WriteString(itemSeparator)

	for _, item := range t {
		switch v := item.(type) {
		case nil:
			// Null is encoded as \x00\xFF
			result.WriteString(nullByte + boundaryByte)
		case string:
			// Unicode strings are encoded as \x02 + UTF8 bytes with null escaped + \x00
			result.WriteString(typeString)
			result.WriteString(escapeNulls([]byte(v)))
			result.WriteString(nullByte)
		case []byte:
			// Byte strings are encoded as \x01 + bytes with null escaped + \x00
			result.WriteString(typeBytes)
			result.WriteString(escapeNulls(v))
			result.WriteString(nullByte)
		case Tuple:
			// Nested tuples are encoded as \x05 + encoded elements + \x00
			result.WriteString(typeNested)
			encoded := v.Encode()
			// Skip the leading separator when nesting
			if len(encoded) > 1 {
				result.WriteString(encoded[1:])
			}
			result.WriteString(nullByte)
		}
	}

	return result.String()
}

// escapeNulls replaces null bytes with \x00\xFF to maintain proper ordering
func escapeNulls(b []byte) string {
	return strings.ReplaceAll(string(b), nullByte, nullByte+boundaryByte)
}

// GetPrefixRange returns (start_key, end_key) for range query to list all tuples
// that match the given prefix items.
func (t Tuple) GetPrefixRange() (string, string) {
	startKey := t.Encode()
	endKey := startKey + boundaryByte
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

// Decode parses an encoded tuple string back into a Tuple.
// Returns error if the encoding is invalid.
func Decode(encoded string) (Tuple, error) {
	if !strings.HasPrefix(encoded, itemSeparator) {
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
func decodeString(encoded string) (string, int, error) {
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
func decodeBytes(encoded string) ([]byte, int, error) {
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
func decodeNested(encoded string) (Tuple, int, error) {
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
