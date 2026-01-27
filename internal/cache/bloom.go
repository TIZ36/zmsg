package cache

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-redis/redis/v8"
)

// BloomFilter 布隆过滤器
type BloomFilter struct {
	redis  *redis.Client
	config *BloomConfig
	filter *bloom.BloomFilter
	mu     sync.RWMutex

	// 本地缓存（提高性能）
	localCache *localCache
	localMu    sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// BloomConfig 布隆过滤器配置
type BloomConfig struct {
	Key                  string
	Capacity             uint
	ErrorRate            float64
	SyncInterval         time.Duration
	EnableLocalCache     bool
	LocalCacheMaxEntries int
	LocalCacheTTL        time.Duration
	DeleteStrategy       string
	RedisTTL             time.Duration
}

const (
	BloomDeleteLocal = "local"
	BloomDeleteClear = "clear"
)

// NewBloomFilter 创建布隆过滤器
func NewBloomFilter(config *BloomConfig, redisClient *redis.Client) *BloomFilter {
	if config == nil {
		config = &BloomConfig{
			Key:                  "zmsg:bloom:filter",
			Capacity:             1000000,
			ErrorRate:            0.01,
			SyncInterval:         30 * time.Second,
			EnableLocalCache:     true,
			LocalCacheMaxEntries: 0,
			LocalCacheTTL:        0,
			DeleteStrategy:       BloomDeleteLocal,
			RedisTTL:             24 * time.Hour,
		}
	}

	if config.DeleteStrategy == "" {
		config.DeleteStrategy = BloomDeleteLocal
	}
	if config.RedisTTL <= 0 {
		config.RedisTTL = 24 * time.Hour
	}

	ctx, cancel := context.WithCancel(context.Background())
	bf := &BloomFilter{
		redis:  redisClient,
		config: config,
		filter: bloom.NewWithEstimates(config.Capacity, config.ErrorRate),
		ctx:    ctx,
		cancel: cancel,
	}

	if config.EnableLocalCache {
		bf.localCache = newLocalCache(config.LocalCacheMaxEntries, config.LocalCacheTTL)
	}

	// 从Redis异步加载布隆过滤器
	go func() {
		if err := bf.loadFromRedis(context.Background()); err != nil {
			// 如果加载失败，保持当前的空过滤器并记录错误
			fmt.Printf("Failed to load bloom filter from Redis: %v\n", err)
		}
	}()

	// 启动同步协程
	if config.SyncInterval > 0 {
		go bf.syncLoop(bf.ctx)
	}

	return bf
}

// Add 添加元素
func (b *BloomFilter) Add(ctx context.Context, key string) error {
	b.mu.Lock()
	b.filter.Add([]byte(key))
	b.mu.Unlock()

	// 更新本地缓存
	if b.config.EnableLocalCache {
		b.localMu.Lock()
		if b.localCache != nil {
			b.localCache.Set(key, true)
		}
		b.localMu.Unlock()
	}

	return nil
}

// AddBatch 批量添加元素
func (b *BloomFilter) AddBatch(ctx context.Context, keys []string) error {
	b.mu.Lock()
	for _, key := range keys {
		b.filter.Add([]byte(key))
	}
	b.mu.Unlock()

	// 更新本地缓存
	if b.config.EnableLocalCache {
		b.localMu.Lock()
		for _, key := range keys {
			if b.localCache != nil {
				b.localCache.Set(key, true)
			}
		}
		b.localMu.Unlock()
	}

	return nil
}

// Test 测试元素是否存在
func (b *BloomFilter) Test(ctx context.Context, key string) bool {
	// 先检查本地缓存（如果启用）
	if b.config.EnableLocalCache {
		b.localMu.RLock()
		exists, found := false, false
		if b.localCache != nil {
			exists, found = b.localCache.Get(key)
		}
		b.localMu.RUnlock()

		if found {
			return exists
		}
	}

	b.mu.RLock()
	exists := b.filter.Test([]byte(key))
	b.mu.RUnlock()

	// 更新本地缓存
	if b.config.EnableLocalCache {
		b.localMu.Lock()
		if b.localCache != nil {
			b.localCache.Set(key, exists)
		}
		b.localMu.Unlock()
	}

	return exists
}

// TestBatch 批量测试元素
func (b *BloomFilter) TestBatch(ctx context.Context, keys []string) (map[string]bool, error) {
	results := make(map[string]bool)

	for _, key := range keys {
		results[key] = b.Test(ctx, key)
	}

	return results, nil
}

// Delete 删除元素（近似删除）
func (b *BloomFilter) Delete(ctx context.Context, key string) error {
	strategy := strings.ToLower(b.config.DeleteStrategy)
	switch strategy {
	case BloomDeleteClear:
		return b.Clear(ctx)
	default:
		// 标准布隆过滤器不支持删除
		// 简单实现：在本地缓存中标记为不存在
		if b.config.EnableLocalCache {
			b.localMu.Lock()
			if b.localCache != nil {
				b.localCache.Set(key, false)
			}
			b.localMu.Unlock()
		}
		return nil
	}
}

// Clear 清空布隆过滤器
func (b *BloomFilter) Clear(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 创建新的布隆过滤器
	b.filter = bloom.NewWithEstimates(b.config.Capacity, b.config.ErrorRate)

	// 清空本地缓存
	if b.config.EnableLocalCache {
		b.localMu.Lock()
		if b.localCache != nil {
			b.localCache.Clear()
		}
		b.localMu.Unlock()
	}

	// 清空Redis中的布隆过滤器
	return b.redis.Del(ctx, b.config.Key).Err()
}

// loadFromRedis 从Redis加载布隆过滤器
func (b *BloomFilter) loadFromRedis(ctx context.Context) error {
	data, err := b.redis.Get(ctx, b.config.Key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil // 不存在是正常的
		}
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.filter.UnmarshalBinary(data)
}

// saveToRedis 保存布隆过滤器到Redis
func (b *BloomFilter) saveToRedis(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, err := b.filter.MarshalBinary()
	if err != nil {
		return err
	}

	// 保存到Redis，设置过期时间（可选）
	return b.redis.Set(ctx, b.config.Key, data, b.config.RedisTTL).Err()
}

// syncLoop 同步循环
func (b *BloomFilter) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(b.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// 退出前保存一次
			_ = b.saveToRedis(context.Background())
			return

		case <-ticker.C:
			// 定期同步到Redis
			if err := b.saveToRedis(context.Background()); err != nil {
				// 记录错误但不中断
				fmt.Printf("Failed to sync bloom filter to Redis: %v\n", err)
			}
		}
	}
}

// Close 停止同步并持久化
func (b *BloomFilter) Close() {
	if b.cancel != nil {
		b.cancel()
	}
	_ = b.saveToRedis(context.Background())
}

type localCacheEntry struct {
	key       string
	value     bool
	expiresAt time.Time
}

type localCache struct {
	maxEntries int
	ttl        time.Duration
	items      map[string]*list.Element
	order      *list.List
}

func newLocalCache(maxEntries int, ttl time.Duration) *localCache {
	return &localCache{
		maxEntries: maxEntries,
		ttl:        ttl,
		items:      make(map[string]*list.Element),
		order:      list.New(),
	}
}

func (c *localCache) Get(key string) (bool, bool) {
	elem, ok := c.items[key]
	if !ok {
		return false, false
	}

	entry := elem.Value.(*localCacheEntry)
	if c.isExpired(entry) {
		c.removeElement(elem)
		return false, false
	}

	c.order.MoveToFront(elem)
	return entry.value, true
}

func (c *localCache) Set(key string, value bool) {
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*localCacheEntry)
		entry.value = value
		entry.expiresAt = c.expireAt()
		c.order.MoveToFront(elem)
		return
	}

	entry := &localCacheEntry{
		key:       key,
		value:     value,
		expiresAt: c.expireAt(),
	}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	c.evictIfNeeded()
}

func (c *localCache) Clear() {
	c.items = make(map[string]*list.Element)
	c.order = list.New()
}

func (c *localCache) Len() int {
	return len(c.items)
}

func (c *localCache) expireAt() time.Time {
	if c.ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.ttl)
}

func (c *localCache) isExpired(entry *localCacheEntry) bool {
	if entry.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(entry.expiresAt)
}

func (c *localCache) evictIfNeeded() {
	if c.maxEntries <= 0 {
		return
	}
	for len(c.items) > c.maxEntries {
		elem := c.order.Back()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}

func (c *localCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*localCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(elem)
}

// GetStats 获取统计信息
func (b *BloomFilter) GetStats() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := make(map[string]interface{})

	// 计算布隆过滤器统计信息
	k := b.filter.K()
	m := b.filter.Cap()
	n := b.filter.ApproximatedSize()

	stats["k"] = k
	stats["m"] = m
	stats["n"] = n
	stats["capacity"] = b.config.Capacity
	stats["error_rate"] = b.config.ErrorRate

	// 计算假阳性概率
	if n > 0 {
		falsePositiveProb := bloom.EstimateFalsePositiveRate(k, m, uint(n))
		stats["false_positive_probability"] = falsePositiveProb
	}

	// 本地缓存大小
	if b.config.EnableLocalCache {
		b.localMu.RLock()
		if b.localCache != nil {
			stats["local_cache_size"] = b.localCache.Len()
		} else {
			stats["local_cache_size"] = 0
		}
		b.localMu.RUnlock()
	}

	return stats
}
