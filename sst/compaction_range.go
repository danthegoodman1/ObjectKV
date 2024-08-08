package sst

type RangeCompactionStrategy struct {
	rangeSplitThresholdBytes int64
	rangeSplitThresholdRows  int64
}

func (r *RangeCompactionStrategy) Init() {

}

func DefaultRangeCompactionStrategy() RangeCompactionStrategy {
	return RangeCompactionStrategy{
		rangeSplitThresholdBytes: 1_000_000,
		rangeSplitThresholdRows:  100_000,
	}
}
