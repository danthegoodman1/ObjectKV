package snapshot_reader

import "github.com/danthegoodman1/objectkv/sst"

type SegmentRecord struct {
	// ID of the segment, should typically be the final file name. Must be sorted by time (freshness)
	// with newer blocks having higher values
	ID string
	// Level is the level of the segment in the LSM. Checked in ascending order.
	Level    int
	Metadata sst.SegmentMetadata
}
