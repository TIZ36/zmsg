package zmsg

const (
	// 默认值
	DefaultL1BufferItems  = 64
	DefaultBatchSize      = 1000
	DefaultFlushThreshold = 1000

	// Redis Key 前缀
	RedisPrefixL2    = "zmsg:l2:"
	RedisPrefixQueue = "zmsg:queue:"
	RedisPrefixBloom = "zmsg:bloom:"
)
