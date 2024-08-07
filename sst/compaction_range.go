package sst

type RangeCompactionStrategy struct {
	rangeSplitThresholdBytes int64
}

func (r *RangeCompactionStrategy) Init() {
	r.rangeSplitThresholdBytes = 1_000_000 // 1MB default
}
