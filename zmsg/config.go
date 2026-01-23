package zmsg

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 配置结构
type Config struct {
	Postgres PostgresConfig `yaml:"postgres" json:"postgres"`
	Redis    RedisConfig    `yaml:"redis" json:"redis"`
	Queue    QueueConfig    `yaml:"queue" json:"queue"`
	Cache    CacheConfig    `yaml:"cache" json:"cache"`
	Batch    BatchConfig    `yaml:"batch" json:"batch"`
	ID       IDConfig       `yaml:"id" json:"id"`
	Log      LogConfig      `yaml:"log" json:"log"`

	// 默认值
	DefaultTTL         time.Duration `yaml:"default_ttl" json:"default_ttl"`
	DefaultConsistency Consistency   `yaml:"default_consistency" json:"default_consistency"`
}

// PostgresConfig PostgreSQL 配置
type PostgresConfig struct {
	DSN          string `yaml:"dsn" json:"dsn"`
	MaxOpenConns int    `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns" json:"max_idle_conns"`
}

// RedisConfig Redis 配置 (L2 缓存)
type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	Password string `yaml:"password" json:"password"`
	DB       int    `yaml:"db" json:"db"`
}

// QueueConfig 队列配置 (asynq)
type QueueConfig struct {
	Addr        string        `yaml:"addr" json:"addr"`
	Password    string        `yaml:"password" json:"password"`
	DB          int           `yaml:"db" json:"db"`
	Concurrency int           `yaml:"concurrency" json:"concurrency"`
	Queues      map[string]int `yaml:"queues" json:"queues"`
	RetryMax    int           `yaml:"retry_max" json:"retry_max"`     // 最大重试次数
	RetryDelay  time.Duration `yaml:"retry_delay" json:"retry_delay"` // 重试延迟
	TaskDelay   time.Duration `yaml:"task_delay" json:"task_delay"`   // 任务延迟执行时间（0=立即）
	FallbackToSyncStoreOnEnqueueFail bool `yaml:"fallback_to_sync_store_on_enqueue_fail" json:"fallback_to_sync_store_on_enqueue_fail"`
}

// CacheConfig 缓存配置
type CacheConfig struct {
	// L1 本地缓存 (ristretto)
	L1MaxCost     int64 `yaml:"l1_max_cost" json:"l1_max_cost"`
	L1NumCounters int64 `yaml:"l1_num_counters" json:"l1_num_counters"`

	// 布隆过滤器
	BloomCapacity  int64   `yaml:"bloom_capacity" json:"bloom_capacity"`
	BloomErrorRate float64 `yaml:"bloom_error_rate" json:"bloom_error_rate"`
	BloomEnableLocalCache bool `yaml:"bloom_enable_local_cache" json:"bloom_enable_local_cache"`
	BloomSyncInterval time.Duration `yaml:"bloom_sync_interval" json:"bloom_sync_interval"`
	BloomRedisTTL      time.Duration `yaml:"bloom_redis_ttl" json:"bloom_redis_ttl"`
	BloomDeleteStrategy string       `yaml:"bloom_delete_strategy" json:"bloom_delete_strategy"`
	BloomLocalCacheMaxEntries int     `yaml:"bloom_local_cache_max_entries" json:"bloom_local_cache_max_entries"`
	BloomLocalCacheTTL        time.Duration `yaml:"bloom_local_cache_ttl" json:"bloom_local_cache_ttl"`

	// 缓存一致性策略
	RollbackL1OnL2Fail bool `yaml:"rollback_l1_on_l2_fail" json:"rollback_l1_on_l2_fail"`
	EnqueueCompensationOnL2Fail bool `yaml:"enqueue_compensation_on_l2_fail" json:"enqueue_compensation_on_l2_fail"`
	CompensationDelay time.Duration `yaml:"compensation_delay" json:"compensation_delay"`
}

// BatchConfig 批处理配置
type BatchConfig struct {
	Size         int           `yaml:"size" json:"size"`
	Interval     time.Duration `yaml:"interval" json:"interval"`              // 周期 flush 间隔
	MaxQueueSize int64         `yaml:"max_queue_size" json:"max_queue_size"` // 队列长度阈值（高并发触发）
	WriterShards int           `yaml:"writer_shards" json:"writer_shards"`
	FlushTimeout time.Duration `yaml:"flush_timeout" json:"flush_timeout"`
}

// IDConfig ID 生成配置
type IDConfig struct {
	Prefix  string        `yaml:"prefix" json:"prefix"`
	NodeTTL time.Duration `yaml:"node_ttl" json:"node_ttl"`
}

// LogConfig 日志配置
type LogConfig struct {
	// Level 日志级别: debug, info, warn, error
	Level string `yaml:"level" json:"level"`

	// Encoding 输出格式: json, console
	Encoding string `yaml:"encoding" json:"encoding"`

	// AddCaller 是否添加调用者信息（文件路径和行号）
	AddCaller bool `yaml:"add_caller" json:"add_caller"`

	// CallerSkip 调用栈跳过层数（默认1）
	CallerSkip int `yaml:"caller_skip" json:"caller_skip"`

	// Development 开发模式（更易读的输出，打印堆栈跟踪）
	Development bool `yaml:"development" json:"development"`

	// DisableStacktrace 禁用堆栈跟踪
	DisableStacktrace bool `yaml:"disable_stacktrace" json:"disable_stacktrace"`

	// StacktraceLevel 启用堆栈跟踪的日志级别: warn, error
	StacktraceLevel string `yaml:"stacktrace_level" json:"stacktrace_level"`

	// OutputPaths 输出路径，支持 stdout, stderr, 或文件路径
	OutputPaths []string `yaml:"output_paths" json:"output_paths"`

	// ErrorOutputPaths 错误输出路径
	ErrorOutputPaths []string `yaml:"error_output_paths" json:"error_output_paths"`

	// TimeFormat 时间格式: iso8601, rfc3339, epoch, millis, nanos
	TimeFormat string `yaml:"time_format" json:"time_format"`

	// ColorOutput 彩色输出（仅 console 模式有效）
	ColorOutput bool `yaml:"color_output" json:"color_output"`

	// MetricsEnabled 是否启用 metrics
	MetricsEnabled bool `yaml:"metrics_enabled" json:"metrics_enabled"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		Postgres: PostgresConfig{
			MaxOpenConns: 25,
			MaxIdleConns: 10,
		},
		Redis: RedisConfig{
			DB: 0,
		},
		Queue: QueueConfig{
			DB:          1,
			Concurrency: 10,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			RetryMax:    3,
			RetryDelay:  5 * time.Second,
			TaskDelay:   0, // 默认立即执行
		},
		Cache: CacheConfig{
			L1MaxCost:      1000000,
			L1NumCounters:  10000000,
			BloomCapacity:  1000000,
			BloomErrorRate: 0.01,
			BloomEnableLocalCache: false,
			BloomSyncInterval: 30 * time.Second,
			BloomRedisTTL:      24 * time.Hour,
			BloomDeleteStrategy: "local",
			BloomLocalCacheMaxEntries: 0,
			BloomLocalCacheTTL:        0,
			RollbackL1OnL2Fail:        false,
			EnqueueCompensationOnL2Fail: false,
			CompensationDelay:           0,
		},
		Batch: BatchConfig{
			Size:         1000,
			Interval:     5 * time.Second,
			MaxQueueSize: 1000, // 默认 1000 个任务触发 flush
			WriterShards: 16,
			FlushTimeout: 30 * time.Second,
		},
		ID: IDConfig{
			Prefix:  "zmsg",
			NodeTTL: 30 * time.Second,
		},
		Log: LogConfig{
			Level:             "info",
			Encoding:          "json",
			AddCaller:         false,
			CallerSkip:        1,
			Development:       false,
			DisableStacktrace: false,
			StacktraceLevel:   "error",
			OutputPaths:       []string{"stdout"},
			ErrorOutputPaths:  []string{"stderr"},
			TimeFormat:        "iso8601",
			ColorOutput:       false,
			MetricsEnabled:    true,
		},
		DefaultTTL:         24 * time.Hour,
		DefaultConsistency: ConsistencyEventual,
	}
}

// LoadConfig 从 YAML 文件加载配置
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse yaml: %w", err)
	}

	return cfg, nil
}

// LoadConfigFromBytes 从字节数据加载配置
func LoadConfigFromBytes(data []byte) (Config, error) {
	cfg := DefaultConfig()

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse yaml: %w", err)
	}

	return cfg, nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Postgres.DSN == "" {
		return ErrInvalidConfig("postgres.dsn is required")
	}
	if c.Redis.Addr == "" {
		return ErrInvalidConfig("redis.addr is required")
	}
	if c.Queue.Addr == "" {
		c.Queue.Addr = c.Redis.Addr // 默认使用同一个 Redis
	}
	if c.Batch.Size <= 0 {
		c.Batch.Size = 1000
	}
	if c.Batch.Interval <= 0 {
		c.Batch.Interval = 5 * time.Second
	}
	if c.Batch.WriterShards <= 0 {
		c.Batch.WriterShards = 16
	}
	if c.Batch.FlushTimeout <= 0 {
		c.Batch.FlushTimeout = 30 * time.Second
	}
	if c.Queue.Concurrency <= 0 {
		c.Queue.Concurrency = 10
	}
	if c.Queue.Queues == nil || len(c.Queue.Queues) == 0 {
		c.Queue.Queues = map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		}
	}
	if c.Cache.BloomSyncInterval <= 0 {
		c.Cache.BloomSyncInterval = 30 * time.Second
	}
	if c.Cache.BloomRedisTTL <= 0 {
		c.Cache.BloomRedisTTL = 24 * time.Hour
	}
	if c.Cache.BloomDeleteStrategy == "" {
		c.Cache.BloomDeleteStrategy = "local"
	}
	if c.Cache.BloomLocalCacheMaxEntries < 0 {
		c.Cache.BloomLocalCacheMaxEntries = 0
	}
	return nil
}
