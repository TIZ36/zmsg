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
	Addr        string `yaml:"addr" json:"addr"`
	Password    string `yaml:"password" json:"password"`
	DB          int    `yaml:"db" json:"db"`
	Concurrency int    `yaml:"concurrency" json:"concurrency"`
}

// CacheConfig 缓存配置
type CacheConfig struct {
	// L1 本地缓存 (ristretto)
	L1MaxCost     int64 `yaml:"l1_max_cost" json:"l1_max_cost"`
	L1NumCounters int64 `yaml:"l1_num_counters" json:"l1_num_counters"`

	// 布隆过滤器
	BloomCapacity  int64   `yaml:"bloom_capacity" json:"bloom_capacity"`
	BloomErrorRate float64 `yaml:"bloom_error_rate" json:"bloom_error_rate"`
}

// BatchConfig 批处理配置
type BatchConfig struct {
	Size           int           `yaml:"size" json:"size"`
	Interval       time.Duration `yaml:"interval" json:"interval"`
	FlushThreshold int64         `yaml:"flush_threshold" json:"flush_threshold"`
}

// IDConfig ID 生成配置
type IDConfig struct {
	Prefix  string        `yaml:"prefix" json:"prefix"`
	NodeTTL time.Duration `yaml:"node_ttl" json:"node_ttl"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level          string `yaml:"level" json:"level"`
	MetricsEnabled bool   `yaml:"metrics_enabled" json:"metrics_enabled"`
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
		},
		Cache: CacheConfig{
			L1MaxCost:      1000000,
			L1NumCounters:  10000000,
			BloomCapacity:  1000000,
			BloomErrorRate: 0.01,
		},
		Batch: BatchConfig{
			Size:           1000,
			Interval:       5 * time.Second,
			FlushThreshold: 1000,
		},
		ID: IDConfig{
			Prefix:  "zmsg",
			NodeTTL: 30 * time.Second,
		},
		Log: LogConfig{
			Level:          "info",
			MetricsEnabled: true,
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
	if c.Queue.Concurrency <= 0 {
		c.Queue.Concurrency = 10
	}
	return nil
}
