package batch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CounterManager 计数器管理器
type CounterManager struct {
	shards    []*CounterShard
	numShards int
	flushCh   chan map[string]int64
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// CounterShard 计数器分片
type CounterShard struct {
	counters map[string]*Counter
	mu       sync.RWMutex
	index    int
}

// Counter 计数器
type Counter struct {
	value   int64
	pending int64
	lastOp  time.Time
	mu      sync.Mutex
}

// NewCounterManager 创建计数器管理器
func NewCounterManager(numShards int) *CounterManager {
	cm := &CounterManager{
		numShards: numShards,
		shards:    make([]*CounterShard, numShards),
		flushCh:   make(chan map[string]int64, 1000),
		stopCh:    make(chan struct{}),
	}

	// 初始化分片
	for i := 0; i < numShards; i++ {
		cm.shards[i] = &CounterShard{
			counters: make(map[string]*Counter),
			index:    i,
		}
	}

	return cm
}

// getShard 获取分片
func (cm *CounterManager) getShard(key string) *CounterShard {
	hash := fnv32(key)
	return cm.shards[hash%uint32(cm.numShards)]
}

// Incr 增加计数
func (cm *CounterManager) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	shard := cm.getShard(key)

	shard.mu.Lock()
	counter, exists := shard.counters[key]
	if !exists {
		counter = &Counter{
			lastOp: time.Now(),
		}
		shard.counters[key] = counter
	}
	shard.mu.Unlock()

	counter.mu.Lock()
	defer counter.mu.Unlock()

	counter.value += delta
	counter.pending += delta
	counter.lastOp = time.Now()

	return counter.value, nil
}

// Decr 减少计数
func (cm *CounterManager) Decr(ctx context.Context, key string, delta int64) (int64, error) {
	return cm.Incr(ctx, key, -delta)
}

// Get 获取计数值
func (cm *CounterManager) Get(key string) (int64, bool) {
	shard := cm.getShard(key)

	shard.mu.RLock()
	counter, exists := shard.counters[key]
	shard.mu.RUnlock()

	if !exists {
		return 0, false
	}

	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.value, true
}

// BatchIncr 批量增加计数
func (cm *CounterManager) BatchIncr(items map[string]int64) map[string]int64 {
	results := make(map[string]int64)

	for key, delta := range items {
		if value, err := cm.Incr(context.Background(), key, delta); err == nil {
			results[key] = value
		}
	}

	return results
}

// Flush 刷新分片
func (cm *CounterManager) Flush(shardIdx int, threshold int64) map[string]int64 {
	if shardIdx < 0 || shardIdx >= cm.numShards {
		return nil
	}

	shard := cm.shards[shardIdx]
	flushed := make(map[string]int64)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	for key, counter := range shard.counters {
		counter.mu.Lock()
		if counter.pending >= threshold {
			flushed[key] = counter.pending
			counter.pending = 0
			counter.lastOp = time.Now()
		}
		counter.mu.Unlock()
	}

	return flushed
}

// FlushAll 刷新所有分片
func (cm *CounterManager) FlushAll(threshold int64) map[string]int64 {
	allFlushed := make(map[string]int64)

	for i := 0; i < cm.numShards; i++ {
		flushed := cm.Flush(i, threshold)
		for key, delta := range flushed {
			allFlushed[key] += delta
		}
	}

	return allFlushed
}

// Start 启动刷新循环
func (cm *CounterManager) Start(ctx context.Context, interval time.Duration, threshold int64) {
	cm.wg.Add(1)
	go cm.flushLoop(ctx, interval, threshold)
}

// flushLoop 刷新循环
func (cm *CounterManager) flushLoop(ctx context.Context, interval time.Duration, threshold int64) {
	defer cm.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// 退出前刷新所有
			cm.flushAllBeforeExit(threshold)
			return

		case <-cm.stopCh:
			cm.flushAllBeforeExit(threshold)
			return

		case <-ticker.C:
			// 定时刷新
			flushed := cm.FlushAll(threshold)
			if len(flushed) > 0 {
				select {
				case cm.flushCh <- flushed:
					// 发送成功
				default:
					// 通道满，记录日志
					fmt.Printf("Counter flush channel full, dropped %d counters\n", len(flushed))
				}
			}
		}
	}
}

// flushAllBeforeExit 退出前刷新所有
func (cm *CounterManager) flushAllBeforeExit(threshold int64) {
	// 刷新所有计数，无论是否达到阈值
	for i := 0; i < cm.numShards; i++ {
		shard := cm.shards[i]
		shard.mu.Lock()
		for key, counter := range shard.counters {
			counter.mu.Lock()
			if counter.pending > 0 {
				flushed := make(map[string]int64)
				flushed[key] = counter.pending

				select {
				case cm.flushCh <- flushed:
					counter.pending = 0
				default:
					// 通道满，无法刷新
				}
			}
			counter.mu.Unlock()
		}
		shard.mu.Unlock()
	}
}

// Stop 停止管理器
func (cm *CounterManager) Stop() {
	close(cm.stopCh)
	cm.wg.Wait()
}

// GetStats 获取统计信息
func (cm *CounterManager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	totalCounters := 0
	totalPending := int64(0)
	totalValue := int64(0)

	for _, shard := range cm.shards {
		shard.mu.RLock()
		totalCounters += len(shard.counters)

		for _, counter := range shard.counters {
			counter.mu.Lock()
			totalPending += counter.pending
			totalValue += counter.value
			counter.mu.Unlock()
		}
		shard.mu.RUnlock()
	}

	stats["total_counters"] = totalCounters
	stats["total_pending"] = totalPending
	stats["total_value"] = totalValue
	stats["num_shards"] = cm.numShards

	return stats
}
