package zmsg

import "time"

const (
	// 默认值
	DefaultL1BufferItems  = 64
	DefaultBatchSize      = 1000
	DefaultFlushThreshold = 1000

	// Redis Key 前缀
	RedisPrefixL2      = "zmsg:l2:"
	RedisPrefixQueue   = "zmsg:queue:"
	RedisPrefixBloom   = "zmsg:bloom:"
	RedisPrefixNode    = "zmsg:node:"
	RedisPrefixCounter = "zmsg:counter:"

	// 队列相关
	QueueTopicSave   = "save"
	QueueTopicDelete = "delete"
	QueueTopicUpdate = "update"
	QueueMaxRetries  = 3
	QueueRetryDelay  = 5 * time.Second

	// ID 生成相关
	SnowflakeEpoch    = 1609459200000 // 2021-01-01 00:00:00 UTC
	SnowflakeTimeBits = 41
	SnowflakeNodeBits = 10
	SnowflakeSeqBits  = 12
	SnowflakeNodeMax  = 1 << 10 // 1024 nodes
	SnowflakeSeqMax   = 1 << 12 // 4096 seq per ms

	// 查询管道步骤
	StepBloomFilter  = "bloom_filter"
	StepL1Cache      = "l1_cache"
	StepSingleFlight = "single_flight"
	StepL2Cache      = "l2_cache"
	StepDatabase     = "database"
	StepBackfill     = "backfill"
)
