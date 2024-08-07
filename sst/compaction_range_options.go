package sst

func WithRangeSplitThreshold(sizeBytes int64) CompactionStrategyOption {
	return func(strategy CompactionStrategy) {
		strat, ok := strategy.(*RangeCompactionStrategy)
		if !ok {
			globalLogger.Fatal().Msg("can only use WithMaxRangeSize with RangeCompactionStrategy")
		}
		strat.rangeSplitThresholdBytes = sizeBytes
	}
}
