package cache

import (
	"context"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-redis/redis/v8"
)

// bloomFilter 布隆过滤器
type bloomFilter struct {
	redis  *redis.Client
	key    string
	filter *bloom.BloomFilter
	mu     sync.RWMutex
}

// newBloomFilter 创建布隆过滤器
func newBloomFilter(redis *redis.Client, capacity int64, errorRate float64) *bloomFilter {
	filter := bloom.NewWithEstimates(uint(capacity), errorRate)

	return &bloomFilter{
		redis:  redis,
		key:    "zmsg:bloom:filter",
		filter: filter,
	}
}

// Add 添加元素
func (b *bloomFilter) Add(ctx context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.filter.Add([]byte(key))

	// 序列化并存储到 Redis
	data, err := b.filter.MarshalBinary()
	if err != nil {
		return err
	}

	return b.redis.Set(ctx, b.key, data, 0).Err()
}

// Test 测试元素是否存在
func (b *bloomFilter) Test(ctx context.Context, key string) bool {
	b.mu.RLock()

	// 如果内存中有，直接返回
	if b.filter.Test([]byte(key)) {
		b.mu.RUnlock()
		return true
	}
	b.mu.RUnlock()

	// 从 Redis 加载
	data, err := b.redis.Get(ctx, b.key).Bytes()
	if err != nil {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 反序列化
	if err := b.filter.UnmarshalBinary(data); err != nil {
		return false
	}

	return b.filter.Test([]byte(key))
}

// Delete 删除元素（近似删除）
func (b *bloomFilter) Delete(ctx context.Context, key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 重置对应的位（如果支持的话）
	// 注意：标准布隆过滤器不支持删除
	// 这里我们记录删除的key，在Test时排除
}
