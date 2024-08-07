package sst

type CompactionStrategy interface {
	// Init should set any default options. Options will be applied after this returns.
	// Anything that errors here should crash the process with logger.Fatal
	Init()
}

type CompactionStrategyOption func(strategy CompactionStrategy)
