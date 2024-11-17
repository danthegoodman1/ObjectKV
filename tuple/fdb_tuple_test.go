package tuple

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestTupleFlatTuples(t *testing.T) {
	tests := []struct {
		name     string
		input    Tuple
		wantErr  bool
		expected Tuple
	}{
		{
			name:     "empty tuple",
			input:    Tuple{},
			wantErr:  false,
			expected: Tuple(nil),
		},
		{
			name:     "basic types",
			input:    Tuple{1, "hello", true, []byte{1, 2, 3}},
			wantErr:  false,
			expected: Tuple{int64(1), "hello", true, []byte{1, 2, 3}},
		},
		{
			name:     "numbers",
			input:    Tuple{int64(-123), uint64(123), 42, -42},
			wantErr:  false,
			expected: Tuple{int64(-123), int64(123), int64(42), int64(-42)},
		},
		{
			name: "big integers",
			input: Tuple{
				big.NewInt(9223372036854775807),
				big.NewInt(-9223372036854775808),
			},
			wantErr: false,
			expected: Tuple{
				int64(9223372036854775807),
				int64(-9223372036854775808),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := tt.input.Pack()
			got, err := Unpack(packed)

			if (err != nil) != tt.wantErr {
				t.Errorf("Unpack() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("Unpack() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTupleNestedTuples(t *testing.T) {
	tests := []struct {
		name     string
		input    Tuple
		wantErr  bool
		expected Tuple
	}{
		{
			name:     "single nested tuple",
			input:    Tuple{1, Tuple{"nested", 42}},
			wantErr:  false,
			expected: Tuple{int64(1), Tuple{"nested", int64(42)}},
		},
		{
			name:     "multiple nested tuples",
			input:    Tuple{Tuple{1, 2}, "middle", Tuple{3, 4}},
			wantErr:  false,
			expected: Tuple{Tuple{int64(1), int64(2)}, "middle", Tuple{int64(3), int64(4)}},
		},
		{
			name:     "deeply nested tuples",
			input:    Tuple{1, Tuple{2, Tuple{3, Tuple{4}}}},
			wantErr:  false,
			expected: Tuple{int64(1), Tuple{int64(2), Tuple{int64(3), Tuple{int64(4)}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := tt.input.Pack()
			got, err := Unpack(packed)

			if (err != nil) != tt.wantErr {
				t.Errorf("Unpack() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("Unpack() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTupleBrokenTuples(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{
			name:    "invalid type code",
			input:   []byte{0xFF, 0x00},
			wantErr: true,
		},
		{
			name:    "truncated float",
			input:   []byte{floatCode, 0x00, 0x00},
			wantErr: true,
		},
		{
			name:    "truncated double",
			input:   []byte{doubleCode, 0x00, 0x00, 0x00, 0x00},
			wantErr: true,
		},
		{
			name:    "truncated UUID",
			input:   []byte{uuidCode, 0x00, 0x00, 0x00},
			wantErr: true,
		},
		{
			name:    "truncated versionstamp",
			input:   []byte{versionstampCode, 0x00, 0x00},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unpack(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unpack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTuplePanicCases(t *testing.T) {
	tests := []struct {
		name      string
		input     Tuple
		wantPanic bool
	}{
		{
			name:      "unsupported type",
			input:     Tuple{complex(1, 2)},
			wantPanic: true,
		},
		{
			name:      "incomplete versionstamp in normal pack",
			input:     Tuple{IncompleteVersionstamp(1)},
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("Pack() panic = %v, wantPanic %v", r, tt.wantPanic)
				}
			}()
			tt.input.Pack()
		})
	}
}

func TestTupleLexicographicalOrdering(t *testing.T) {
	// Create tuples from path segments
	paths := []Tuple{
		{"dir", "b"},      // /dir/b
		{"dir"},           // /dir
		{"dir", "a", "1"}, // /dir/a/1
		{"dir", "a"},      // /dir/a
	}

	// Pack all tuples
	packed := make([][]byte, len(paths))
	for i, path := range paths {
		packed[i] = path.Pack()
	}

	// Sort the packed bytes
	sortedPacked := make([][]byte, len(packed))
	copy(sortedPacked, packed)
	sort.Slice(sortedPacked, func(i, j int) bool {
		return bytes.Compare(sortedPacked[i], sortedPacked[j]) < 0
	})

	// Unpack and convert back to strings for easier verification
	got := make([]string, len(sortedPacked))
	for i, p := range sortedPacked {
		tuple, err := Unpack(p)
		if err != nil {
			t.Fatalf("Failed to unpack tuple: %v", err)
		}
		got[i] = "/" + strings.Join(convertTupleToStrings(tuple), "/")
	}

	// Expected order: root first, then alphabetical segments, then numeric segments
	want := []string{
		"/dir",
		"/dir/a",
		"/dir/a/1",
		"/dir/b",
	}

	// Verify ordering
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Incorrect lexicographical ordering\ngot:  %v\nwant: %v", got, want)
	}

	// Additional verification that /dir/a/1 is specifically last
	if got[len(got)-1] != "/dir/b" {
		t.Errorf("Expected /dir/b to be last, but got %s", got[len(got)-1])
	}
}

// Helper function to convert tuple elements to strings
func convertTupleToStrings(t Tuple) []string {
	result := make([]string, len(t))
	for i, v := range t {
		result[i] = fmt.Sprint(v)
	}
	return result
}
