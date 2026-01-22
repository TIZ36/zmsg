package batch

import (
	"context"
	"sync"
	"time"
)

// Counter 计数器
type Counter struct {
	value     int64
	pending   int64
	mu        sync.Mutex
	lastFlush time.Time
}

// NewCounter 创建计数器
func NewCounter() *Counter {
	return &Counter{
		lastFlush: time.Now(),
	}
}

// Incr 增加计数
func (c *Counter) Incr(delta int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.value += delta
	c.pending += delta

	return c.value, nil
}

// GetValue 获取当前值
func (c *Counter) GetValue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// GetPending 获取待刷新的值
func (c *Counter) GetPending() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending
}

// ResetPending 重置待刷新值
func (c *Counter) ResetPending() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := c.pending
	c.pending = 0
	c.lastFlush = time.Now()

	return pending
}

// batchCounter 批处理计数器
type batchCounter struct {
	counters sync.Map // key -> *Counter
	flushCh  chan flushRequest
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

type flushRequest struct {
	key   string
	delta int64
}

// newBatchCounter 创建批处理计数器
func newBatchCounter() *batchCounter {
	return &batchCounter{
		flushCh: make(chan flushRequest, 10000),
		stopCh:  make(chan struct{}),
	}
}

// Incr 增加计数
func (b *batchCounter) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	// 获取或创建计数器
	counter, _ := b.counters.LoadOrStore(key, NewCounter())

	value, err := counter.(*Counter).Incr(delta)
	if err != nil {
		return 0, err
	}

	// 如果待刷新值超过阈值，发送刷新请求
	pending := counter.(*Counter).GetPending()
	if pending >= 1000 { // 可配置
		select {
		case b.flushCh <- flushRequest{key: key, delta: pending}:
			counter.(*Counter).ResetPending()
		default:
			// 队列满，记录日志
		}
	}

	return value, nil
}

// BatchGet 批量获取计数值
func (b *batchCounter) BatchGet(keys []string) (map[string]int64, error) {
	result := make(map[string]int64)

	for _, key := range keys {
		if counter, ok := b.counters.Load(key); ok {
			result[key] = counter.(*Counter).GetValue()
		}
	}

	return result, nil
}

// flushLoop 刷新循环
func (b *batchCounter) flushLoop(ctx context.Context,
	flusher func(map[string]int64) error) {

	defer b.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batch := make(map[string]int64)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := flusher(batch); err != nil {
			// 记录错误，可以重试
		}

		// 清空批次
		for key := range batch {
			delete(batch, key)
		}
	}

	for {
		select {
		case <-b.stopCh:
			flushBatch()
			return

		case req := <-b.flushCh:
			batch[req.key] += req.delta

			// 批量大小限制
			if len(batch) >= 1000 {
				flushBatch()
			}

		case <-ticker.C:
			flushBatch()
		}
	}
}

// Stop 停止
func (b *batchCounter) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}
