package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-redis/redis/v8"
)

// CacheLayer 缓存层
type CacheLayer struct {
	l1    *ristretto.Cache
	l2    *redis.Client
	mu    sync.RWMutex
	stats *CacheStats
}

// CacheStats 缓存统计
type CacheStats struct {
	L1Hits   int64
	L1Misses int64
	L2Hits   int64
	L2Misses int64
}

// NewCacheLayer 创建缓存层
func NewCacheLayer(l1 *ristretto.Cache, l2 *redis.Client) *CacheLayer {
	return &CacheLayer{
		l1:    l1,
		l2:    l2,
		stats: &CacheStats{},
	}
}

// Get 获取数据
func (c *CacheLayer) Get(ctx context.Context, key string) ([]byte, error) {
	// L1 检查
	if val, found := c.l1.Get(key); found {
		c.stats.L1Hits++
		return val.([]byte), nil
	}
	c.stats.L1Misses++

	// L2 检查
	val, err := c.l2.Get(ctx, key).Bytes()
	if err == nil {
		// 回填 L1
		c.l1.SetWithTTL(key, val, int64(len(val)), time.Hour)
		c.stats.L2Hits++
		return val, nil
	}

	if err == redis.Nil {
		c.stats.L2Misses++
		return nil, fmt.Errorf("key not found")
	}

	return nil, err
}

// Set 设置数据
func (c *CacheLayer) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// L1
	c.l1.SetWithTTL(key, value, int64(len(value)), ttl)

	// L2
	return c.l2.Set(ctx, key, value, ttl).Err()
}

// Delete 删除数据
func (c *CacheLayer) Delete(ctx context.Context, key string) error {
	c.l1.Del(key)
	return c.l2.Del(ctx, key).Err()
}

// BatchSet 批量设置
func (c *CacheLayer) BatchSet(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	pipe := c.l2.Pipeline()

	for key, value := range items {
		// L1
		c.l1.SetWithTTL(key, value, int64(len(value)), ttl)

		// L2 pipeline
		pipe.Set(ctx, key, value, ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// Stats 获取统计信息
func (c *CacheLayer) Stats() *CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &CacheStats{
		L1Hits:   c.stats.L1Hits,
		L1Misses: c.stats.L1Misses,
		L2Hits:   c.stats.L2Hits,
		L2Misses: c.stats.L2Misses,
	}
}
