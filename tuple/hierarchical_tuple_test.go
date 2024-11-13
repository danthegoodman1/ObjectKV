package tuple

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestHierarchicalTupleOrdering(t *testing.T) {
	// Create tuples to test
	dir, err := HierarchicalTuple{[]byte("dir")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\")}.Pack() failed: %v", err)
	}
	dirA, err := HierarchicalTuple{[]byte("dir"), []byte("a")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\")}.Pack() failed: %v", err)
	}
	dirB, err := HierarchicalTuple{[]byte("dir"), []byte("b")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"b\")}.Pack() failed: %v", err)
	}
	dirUnicode, err := HierarchicalTuple{[]byte("dir"), []byte("🚨")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"🚨\")}.Pack() failed: %v", err)
	}
	dirA1, err := HierarchicalTuple{[]byte("dir"), []byte("a"), []byte("1")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\"), []byte(\"1\")}.Pack() failed: %v", err)
	}

	dir2, err := HierarchicalTuple{[]byte("dir2")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\"), []byte(\"1\")}.Pack() failed: %v", err)
	}

	// Create all keys we want to test ordering of
	keys := [][]byte{
		dir,        // The directory itself
		dir2,       // same level
		dirA,       // Should be included
		dirB,       // Should be included
		dirUnicode, // Should be included
		dirA1,      // Deeper entry
	}

	// Print keys in lexicographic order for visual verification
	fmt.Println("Keys in hierarchical order:")
	for _, key := range keys {
		fmt.Printf("  %q\n", key)
	}

	// Verify ordering
	for i := 0; i < len(keys)-1; i++ {
		if bytes.Compare(keys[i], keys[i+1]) > 0 {
			t.Errorf("Keys not in correct order at position %d and %d, %q > %q", i, i+1, keys[i], keys[i+1])
			// Print the real order
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i], keys[j]) < 0
			})
			fmt.Println("Keys in real order:")
			for _, key := range keys {
				fmt.Printf("  %q\n", key)
			}
		}
	}

}

func TestHierarchicalInvalidInput(t *testing.T) {
	// Test cases with invalid inputs
	testCases := []struct {
		name    string
		input   []byte
		wantErr string
	}{
		{
			name:    "plain string",
			input:   Tuple{"hello"}.Pack(),
			wantErr: "invalid hierarchical element",
		},
		{
			name:    "number",
			input:   Tuple{123, "abc"}.Pack(),
			wantErr: "invalid hierarchical element",
		},
		{
			name:    "not hierarchical",
			input:   Tuple{[]byte("hey"), []byte("ho")}.Pack(),
			wantErr: "first element did not start with hierarchical byte",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("%q", tc.input)
			_, err := DecodeHierarchical(tc.input)
			if err == nil {
				t.Errorf("DecodeHierarchical(%q) expected error, got nil", tc.input)
				return
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("DecodeHierarchical(%q) error = %v, want %v", tc.input, err, tc.wantErr)
				return
			}
		})
	}
}

func TestHierarchicalRangeKeys(t *testing.T) {
	// Create tuples to test
	dir, err := HierarchicalTuple{[]byte("dir")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\")}.Pack() failed: %v", err)
	}
	dirA, err := HierarchicalTuple{[]byte("dir"), []byte("a")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\")}.Pack() failed: %v", err)
	}
	dirB, err := HierarchicalTuple{[]byte("dir"), []byte("b")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"b\")}.Pack() failed: %v", err)
	}
	dirA1, err := HierarchicalTuple{[]byte("dir"), []byte("a"), []byte("1")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\"), []byte(\"1\")}.Pack() failed: %v", err)
	}
	dir2, err := HierarchicalTuple{[]byte("dir2")}.Pack()
	if err != nil {
		t.Fatalf("HierarchicalTuple{[]byte(\"dir\"), []byte(\"a\"), []byte(\"1\")}.Pack() failed: %v", err)
	}

	startRange, endRange, err := HierarchicalTuple{[]byte("dir")}.RangeKeys()
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(dir, startRange) >= 0 {
		t.Fatalf("start range %q was not greater than dir %q", startRange, dir)
	}
	if bytes.Compare(dir, endRange) >= 0 {
		t.Fatalf("end range %q was not greater than dir %q", endRange, dir)
	}

	if bytes.Compare(startRange, dirA) > 0 {
		t.Fatalf("start range %q was greater than dirA %q", startRange, dirA)
	}
	if bytes.Compare(startRange, dirB) > 0 {
		t.Fatalf("start range %q was greater than dirB %q", startRange, dirB)
	}
	if bytes.Compare(startRange, dirA1) > 0 {
		t.Fatalf("start range %q was greater than dirA1 %q", startRange, dirA1)
	}

	if bytes.Compare(endRange, dirA) <= 0 {
		t.Fatalf("end range %q was not greater than dirA %q", endRange, dirA)
	}
	if bytes.Compare(endRange, dirB) <= 0 {
		t.Fatalf("end range %q was not greater than dirB %q", endRange, dirB)
	}
	if bytes.Compare(endRange, dirA1) >= 0 {
		t.Fatalf("end range %q was not less than dirA1 %q", endRange, dirA1)
	}
	if bytes.Compare(endRange, dir2) <= 0 {
		t.Fatalf("end range %q was not less than dir2 %q", endRange, dirA1)
	}
}
