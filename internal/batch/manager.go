package batch

import (
	"context"
	"sync"
	"time"
)

// Manager 批处理管理器
type Manager struct {
	size      int           // 批处理大小
	interval  time.Duration // 刷新间隔
	threshold int64         // 刷新阈值

	aggregator *Aggregator
	shards     []*Shard
	flushCh    chan *Batch
	stopCh     chan struct{}
	wg         sync.WaitGroup

	stats *BatchStats
}

// Batch 批处理
type Batch struct {
	Type    BatchType
	Items   []BatchItem
	Created time.Time
}

// BatchItem 批处理项
type BatchItem struct {
	Key       string
	Value     []byte
	Operation string // "incr", "set", "delete"
	Delta     int64
	SQLTask   interface{}
}

// BatchType 批处理类型
type BatchType int

const (
	BatchTypeCounter BatchType = iota
	BatchTypeContent
	BatchTypeRelation
)

// BatchStats 批处理统计
type BatchStats struct {
	TotalBatches  int64
	TotalItems    int64
	AvgBatchSize  float64
	LastFlushTime time.Time
	PendingItems  int64
}

// NewManager 创建批处理管理器
func NewManager(size int, interval time.Duration, threshold int64) *Manager {
	m := &Manager{
		size:      size,
		interval:  interval,
		threshold: threshold,
		flushCh:   make(chan *Batch, 1000),
		stopCh:    make(chan struct{}),
		stats:     &BatchStats{},
	}

	// 初始化聚合器
	m.aggregator = NewAggregator(threshold)

	// 初始化分片
	m.initShards(16) // 16个分片

	return m
}

// initShards 初始化分片
func (m *Manager) initShards(numShards int) {
	m.shards = make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		m.shards[i] = NewShard(i, m.size, m.flushCh)
	}
}

// Enqueue 入队
func (m *Manager) Enqueue(item BatchItem) error {
	// 根据key选择分片
	shard := m.getShard(item.Key)

	// 加入分片
	return shard.Add(item)
}

// getShard 获取分片
func (m *Manager) getShard(key string) *Shard {
	// 简单的哈希分片
	hash := fnv32(key)
	return m.shards[hash%uint32(len(m.shards))]
}

// Start 启动管理器
func (m *Manager) Start(ctx context.Context) {
	m.wg.Add(2)

	// 启动分片处理器
	go m.processShards(ctx)

	// 启动刷新器
	go m.flushLoop(ctx)
}

// processShards 处理分片
func (m *Manager) processShards(ctx context.Context) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		default:
			// 定期检查分片是否需要刷新
			time.Sleep(time.Millisecond * 100)

			for _, shard := range m.shards {
				if shard.ShouldFlush() {
					shard.Flush()
				}
			}
		}
	}
}

// flushLoop 刷新循环
func (m *Manager) flushLoop(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.flushAll()
			return

		case <-m.stopCh:
			m.flushAll()
			return

		case batch := <-m.flushCh:
			m.processBatch(batch)

		case <-ticker.C:
			// 定时强制刷新
			m.flushAll()
		}
	}
}

// processBatch 处理批处理
func (m *Manager) processBatch(batch *Batch) {
	// 更新统计
	m.stats.TotalBatches++
	m.stats.TotalItems += int64(len(batch.Items))
	m.stats.LastFlushTime = time.Now()

	if m.stats.TotalBatches > 0 {
		m.stats.AvgBatchSize = float64(m.stats.TotalItems) / float64(m.stats.TotalBatches)
	}

	// 聚合处理
	switch batch.Type {
	case BatchTypeCounter:
		m.processCounterBatch(batch)
	case BatchTypeContent:
		m.processContentBatch(batch)
	case BatchTypeRelation:
		m.processRelationBatch(batch)
	}
}

// processCounterBatch 处理计数器批处理
func (m *Manager) processCounterBatch(batch *Batch) {
	// 使用聚合器合并计数
	counters := make(map[string]int64)

	for _, item := range batch.Items {
		if item.Operation == "incr" {
			counters[item.Key] += item.Delta
		}
	}

	// 批量更新数据库
	m.aggregator.FlushCounters(counters)
}

// processContentBatch 处理内容批处理
func (m *Manager) processContentBatch(batch *Batch) {
	// 批量插入内容
	// 这里可以优化为批量 SQL
}

// processRelationBatch 处理关系批处理
func (m *Manager) processRelationBatch(batch *Batch) {
	// 批量处理关系
}

// flushAll 刷新所有分片
func (m *Manager) flushAll() {
	for _, shard := range m.shards {
		shard.Flush()
	}
}

// Stop 停止管理器
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// GetStats 获取统计信息
func (m *Manager) GetStats() *BatchStats {
	// 计算待处理项
	pending := int64(0)
	for _, shard := range m.shards {
		pending += int64(shard.Size())
	}
	m.stats.PendingItems = pending

	return m.stats
}

// fnv32 FNV哈希函数
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)

	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}

	return hash
}
