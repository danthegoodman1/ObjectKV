package tuple

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFlatTuple(t *testing.T) {
	// Test cases with flat tuples
	tests := []struct {
		name string
		in   Tuple
	}{
		{
			name: "basic types",
			in:   Pack("hello", nil, []byte{1, 2, 3}),
		},
		{
			name: "with null bytes in string",
			in:   Pack("hello\x00world", "test"),
		},
		{
			name: "empty string and nil",
			in:   Pack("", nil, ""),
		},
		{
			name: "unicode strings",
			in:   Pack("hello", "世界", "🌍"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the tuple
			encoded := tt.in.Encode()

			// Decode it back
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Compare original and decoded
			if !reflect.DeepEqual(tt.in, decoded) {
				t.Errorf("tuple roundtrip failed\nwant: %#v\ngot:  %#v", tt.in, decoded)
			}
		})
	}
}

func TestNestedTuple(t *testing.T) {
	tests := []struct {
		name string
		in   Tuple
	}{
		{
			name: "simple nesting",
			in: Pack(
				"parent",
				Pack("child1", "child2"),
				"sibling",
			),
		},
		{
			name: "deep nesting",
			in: Pack(
				"root",
				Pack(
					"level1",
					Pack("level2", nil),
					"level1-sibling",
				),
				"root-sibling",
			),
		},
		{
			name: "mixed types nesting",
			in: Pack(
				[]byte{1, 2},
				Pack(
					"nested",
					nil,
					Pack("deep", []byte{3, 4}),
				),
			),
		},
		{
			name: "empty nested tuples",
			in: Pack(
				"parent",
				Pack(),
				Pack(Pack()),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the tuple
			encoded := tt.in.Encode()

			// Decode it back
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v\nencoded value: %q", err, encoded)
			}

			// Compare original and decoded
			if !reflect.DeepEqual(tt.in, decoded) {
				t.Errorf("tuple roundtrip failed\nwant: %#v\ngot:  %#v", tt.in, decoded)
			}

			// Test prefix range
			start, end := tt.in.GetPrefixRange()
			if bytes.Compare(start, end) >= 0 {
				t.Error("prefix range start should be less than end")
			}
		})
	}
}

func TestInvalidDecode(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
	}{
		{
			name:    "empty bytes",
			encoded: []byte{},
		},
		{
			name:    "missing separator prefix",
			encoded: []byte("hello"),
		},
		{
			name:    "incomplete null",
			encoded: []byte("/\x00"),
		},
		{
			name:    "invalid type code",
			encoded: []byte("/\x09hello\x00"),
		},
		{
			name:    "unterminated string",
			encoded: []byte("/\x02hello"),
		},
		{
			name:    "unterminated nested tuple",
			encoded: []byte("/\x05\x02hello\x00"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.encoded)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestSingleItemTuple(t *testing.T) {
	tests := []struct {
		name string
		in   Tuple
	}{
		{
			name: "single string",
			in:   Pack("hello"),
		},
		{
			name: "single nil",
			in:   Pack(nil),
		},
		{
			name: "single bytes",
			in:   Pack([]byte{1, 2, 3}),
		},
		{
			name: "single nested tuple",
			in:   Pack(Pack("nested", "value")),
		},
		{
			name: "single empty nested tuple",
			in:   Pack(Pack()),
		},
		{
			name: "single deep nested tuple",
			in:   Pack(Pack(Pack("deep", nil))),
		},
		{
			name: "unicode string",
			in:   Pack("世界"),
		},
		{
			name: "string with null byte",
			in:   Pack("hello\x00world"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the tuple
			encoded := tt.in.Encode()

			// Decode it back
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v\nencoded value: %q", err, encoded)
			}

			// Compare original and decoded
			if !reflect.DeepEqual(tt.in, decoded) {
				t.Errorf("tuple roundtrip failed\nwant: %#v\ngot:  %#v", tt.in, decoded)
			}

			// Test prefix range
			start, end := tt.in.GetPrefixRange()
			if bytes.Compare(start, end) >= 0 {
				t.Error("prefix range start should be less than end")
			}
		})
	}
}
