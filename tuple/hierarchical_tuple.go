package tuple

import (
	"errors"
	"fmt"
	"reflect"
)

type HierarchicalTuple []any

// Pack creates a tuple using byte elements
func (ht HierarchicalTuple) Pack() ([]byte, error) {
	return ht.pack(1)
}

func (ht HierarchicalTuple) pack(skip int) ([]byte, error) {
	if len(ht) == 0 {
		// treat it as normal
		t := Tuple{}
		for _, element := range ht {
			t = append(t, element)
		}
		return t.Pack(), nil
	}

	temp := make([][]byte, len(ht))
	// Validate and convert elements
	for i, element := range ht {
		switch v := element.(type) {
		case []byte:
			temp[i] = v
		case string:
			temp[i] = []byte(v)
		default:
			return nil, fmt.Errorf("%w: got %T at index %d", ErrInvalidHierarchicalElement, element, i)
		}
	}

	for i := 0; i < len(temp)-skip; i++ {
		// For all but the last item, we must append 0xff
		temp[i] = append([]byte{0xff}, temp[i]...)
	}

	t := Tuple{}
	for _, element := range temp {
		t = append(t, element)
	}
	return t.Pack(), nil
}

var ErrInvalidHierarchicalElement = errors.New("invalid hierarchical element")

func DecodeHierarchical(b []byte) (HierarchicalTuple, error) {
	tuple, err := Unpack(b)
	if err != nil {
		return nil, fmt.Errorf("error in Unpack: %w", err)
	}

	temp := make([]any, len(tuple))

	for i, element := range tuple {
		if b, ok := element.([]byte); ok {
			temp[i] = b
			continue
		}
		return nil, fmt.Errorf("%w: got %s", ErrInvalidHierarchicalElement, reflect.TypeOf(element))
	}

	if len(temp) > 1 && temp[0].([]byte)[0] != 0xff {
		return nil, fmt.Errorf("%w: first element did not start with hierarchical byte", ErrInvalidHierarchicalElement)
	}

	for i := 0; i < len(temp)-1; i++ {
		// For all but the last item, we must rip off the trailing 0xff
		temp[i] = temp[i].([]byte)[1:]
	}

	return temp, nil
}

func (ht HierarchicalTuple) RangeKeys() (start []byte, end []byte, err error) {

	// Create the start and end ranges
	start, err = ht.pack(0)
	if err != nil {
		err = fmt.Errorf("error packing start range: %w", err)
		return
	}

	temp := make(HierarchicalTuple, len(ht))
	copy(temp, ht)
	temp = append(temp, []byte{0x00})

	end, err = temp.pack(0)
	if err != nil {
		err = fmt.Errorf("error packing start range: %w", err)
		return
	}

	return
}
