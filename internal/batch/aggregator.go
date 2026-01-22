package batch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Aggregator 聚合器
type Aggregator struct {
	threshold int64
	counters  map[string]*CounterAggregate
	mu        sync.RWMutex
	flushCh   chan map[string]int64
	stopCh    chan struct{}
}

// CounterAggregate 计数器聚合
type CounterAggregate struct {
	total     int64
	pending   int64
	lastFlush time.Time
	mu        sync.Mutex
}

// NewAggregator 创建聚合器
func NewAggregator(threshold int64) *Aggregator {
	a := &Aggregator{
		threshold: threshold,
		counters:  make(map[string]*CounterAggregate),
		flushCh:   make(chan map[string]int64, 100),
	}

	return a
}

// Add 添加计数
func (a *Aggregator) Add(key string, delta int64) (int64, error) {
	a.mu.Lock()
	counter, exists := a.counters[key]
	if !exists {
		counter = &CounterAggregate{
			lastFlush: time.Now(),
		}
		a.counters[key] = counter
	}
	a.mu.Unlock()

	counter.mu.Lock()
	defer counter.mu.Unlock()

	counter.total += delta
	counter.pending += delta

	// 检查是否需要刷新
	if counter.pending >= a.threshold {
		select {
		case a.flushCh <- map[string]int64{key: counter.pending}:
			counter.pending = 0
			counter.lastFlush = time.Now()
		default:
			// 通道满，记录日志
		}
	}

	return counter.total, nil
}

// Get 获取计数
func (a *Aggregator) Get(key string) (int64, bool) {
	a.mu.RLock()
	counter, exists := a.counters[key]
	a.mu.RUnlock()

	if !exists {
		return 0, false
	}

	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.total, true
}

// FlushCounters 刷新计数器
func (a *Aggregator) FlushCounters(counters map[string]int64) {
	if len(counters) == 0 {
		return
	}

	// 这里应该调用存储层批量更新数据库
	// 例如: store.BatchUpdateCounters(counters)

	// 更新聚合器状态
	a.mu.Lock()
	for key, delta := range counters {
		if counter, exists := a.counters[key]; exists {
			counter.mu.Lock()
			counter.pending -= delta
			if counter.pending < 0 {
				counter.pending = 0
			}
			counter.lastFlush = time.Now()
			counter.mu.Unlock()
		}
	}
	a.mu.Unlock()
}

// FlushAll 刷新所有
func (a *Aggregator) FlushAll() map[string]int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[string]int64)

	for key, counter := range a.counters {
		counter.mu.Lock()
		if counter.pending > 0 {
			result[key] = counter.pending
			counter.pending = 0
			counter.lastFlush = time.Now()
		}
		counter.mu.Unlock()
	}

	return result
}

// Start 启动聚合器
func (a *Aggregator) Start(ctx context.Context, flusher func(map[string]int64) error) {
	go a.flushLoop(ctx, flusher)
}

// flushLoop 刷新循环
func (a *Aggregator) flushLoop(ctx context.Context, flusher func(map[string]int64) error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batch := make(map[string]int64)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := flusher(batch); err != nil {
			// 记录错误，可以重试
			fmt.Printf("Failed to flush batch: %v\n", err)
		}

		// 清理已刷新的计数
		a.cleanupFlushed(batch)

		// 重置批次
		batch = make(map[string]int64)
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			return

		case <-a.stopCh:
			flushBatch()
			return

		case counters := <-a.flushCh:
			// 合并到当前批次
			for key, delta := range counters {
				batch[key] += delta
			}

			// 批次大小限制
			if len(batch) >= 1000 {
				flushBatch()
			}

		case <-ticker.C:
			// 定时刷新
			flushBatch()

			// 检查长时间未刷新的计数器
			a.checkStaleCounters()
		}
	}
}

// cleanupFlushed 清理已刷新的计数
func (a *Aggregator) cleanupFlushed(flushed map[string]int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key := range flushed {
		delete(a.counters, key)
	}
}

// checkStaleCounters 检查过期计数器
func (a *Aggregator) checkStaleCounters() {
	a.mu.RLock()
	defer a.mu.RUnlock()

	staleTime := time.Now().Add(-10 * time.Minute)

	for key, counter := range a.counters {
		counter.mu.Lock()
		if counter.lastFlush.Before(staleTime) && counter.pending == 0 {
			// 长时间未活动，可以清理
			delete(a.counters, key)
		}
		counter.mu.Unlock()
	}
}

// Stop 停止聚合器
func (a *Aggregator) Stop() {
	close(a.stopCh)
}

// GetStats 获取统计信息
func (a *Aggregator) GetStats() map[string]interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_counters"] = len(a.counters)

	pending := int64(0)
	for _, counter := range a.counters {
		counter.mu.Lock()
		pending += counter.pending
		counter.mu.Unlock()
	}

	stats["pending_total"] = pending
	return stats
}
