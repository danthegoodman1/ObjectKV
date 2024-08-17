package snapshot_reader

import "github.com/danthegoodman1/objectkv/sst"

type SegmentRecord struct {
	// ID of the segment, should typically be the final file name
	ID       string
	Metadata sst.SegmentMetadata
}
