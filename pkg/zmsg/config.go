package zmsg

import "time"

// Config 配置结构
type Config struct {
	// PostgreSQL 配置
	PostgresDSN  string `yaml:"postgres_dsn" json:"postgres_dsn"`
	MaxOpenConns int    `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns" json:"max_idle_conns"`

	// Redis 配置 (L2 缓存)
	RedisAddr     string `yaml:"redis_addr" json:"redis_addr"`
	RedisPassword string `yaml:"redis_password" json:"redis_password"`
	RedisDB       int    `yaml:"redis_db" json:"redis_db"`

	// 队列 Redis 配置（独立实例）
	QueueAddr     string `yaml:"queue_addr" json:"queue_addr"`
	QueuePassword string `yaml:"queue_password" json:"queue_password"`
	QueueDB       int    `yaml:"queue_db" json:"queue_db"`

	// L1 缓存配置
	L1MaxCost     int64 `yaml:"l1_max_cost" json:"l1_max_cost"`
	L1NumCounters int64 `yaml:"l1_num_counters" json:"l1_num_counters"`

	// 布隆过滤器配置
	BloomCapacity  int64   `yaml:"bloom_capacity" json:"bloom_capacity"`
	BloomErrorRate float64 `yaml:"bloom_error_rate" json:"bloom_error_rate"`

	// 批处理配置
	BatchSize      int           `yaml:"batch_size" json:"batch_size"`
	BatchInterval  time.Duration `yaml:"batch_interval" json:"batch_interval"`
	FlushThreshold int64         `yaml:"flush_threshold" json:"flush_threshold"`

	// ID 生成配置
	IDPrefix string        `yaml:"id_prefix" json:"id_prefix"`
	NodeTTL  time.Duration `yaml:"node_ttl" json:"node_ttl"`

	// 默认值
	DefaultTTL         time.Duration `yaml:"default_ttl" json:"default_ttl"`
	DefaultConsistency Consistency   `yaml:"default_consistency" json:"default_consistency"`

	// 监控配置
	MetricsEnabled bool   `yaml:"metrics_enabled" json:"metrics_enabled"`
	LogLevel       string `yaml:"log_level" json:"log_level"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		MaxOpenConns:       25,
		MaxIdleConns:       10,
		RedisDB:            0,
		QueueDB:            1,
		L1MaxCost:          1000000,
		L1NumCounters:      10000000,
		BloomCapacity:      1000000,
		BloomErrorRate:     0.01,
		BatchSize:          1000,
		BatchInterval:      5 * time.Second,
		FlushThreshold:     1000,
		IDPrefix:           "zmsg",
		NodeTTL:            30 * time.Second,
		DefaultTTL:         24 * time.Hour,
		DefaultConsistency: ConsistencyEventual,
		MetricsEnabled:     true,
		LogLevel:           "info",
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.PostgresDSN == "" {
		return ErrInvalidConfig("postgres_dsn is required")
	}
	if c.RedisAddr == "" {
		return ErrInvalidConfig("redis_addr is required")
	}
	if c.QueueAddr == "" {
		c.QueueAddr = c.RedisAddr // 默认使用同一个 Redis
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.BatchInterval <= 0 {
		c.BatchInterval = 5 * time.Second
	}
	return nil
}
