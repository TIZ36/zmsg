package batch

import (
	"sync"
	"time"
)

// Shard 分片
type Shard struct {
	id        int
	size      int
	items     []BatchItem
	mu        sync.RWMutex
	lastFlush time.Time
	flushCh   chan<- *Batch
	batchType BatchType
}

// NewShard 创建分片
func NewShard(id, size int, flushCh chan<- *Batch) *Shard {
	return &Shard{
		id:        id,
		size:      size,
		items:     make([]BatchItem, 0, size),
		lastFlush: time.Now(),
		flushCh:   flushCh,
		batchType: BatchTypeContent,
	}
}

// Add 添加项目
func (s *Shard) Add(item BatchItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 确定批处理类型
	s.determineType(item)

	// 添加到分片
	s.items = append(s.items, item)

	return nil
}

// determineType 确定批处理类型
func (s *Shard) determineType(item BatchItem) {
	if item.Operation == "incr" {
		s.batchType = BatchTypeCounter
	} else if item.SQLTask != nil {
		// 这里可以根据SQL内容判断类型
		s.batchType = BatchTypeContent
	}
}

// Size 获取分片大小
func (s *Shard) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// ShouldFlush 是否应该刷新
func (s *Shard) ShouldFlush() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查大小
	if len(s.items) >= s.size {
		return true
	}

	// 检查时间（超过5秒）
	if time.Since(s.lastFlush) > 5*time.Second {
		return true
	}

	return false
}

// Flush 刷新分片
func (s *Shard) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return
	}

	// 创建批处理
	batch := &Batch{
		Type:    s.batchType,
		Items:   make([]BatchItem, len(s.items)),
		Created: time.Now(),
	}

	copy(batch.Items, s.items)

	// 发送到刷新通道
	select {
	case s.flushCh <- batch:
		// 清空分片
		s.items = s.items[:0]
		s.lastFlush = time.Now()
	default:
		// 通道满，保留数据等待下次刷新
	}
}

// GetItems 获取项目（用于测试）
func (s *Shard) GetItems() []BatchItem {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]BatchItem, len(s.items))
	copy(items, s.items)

	return items
}

// Reset 重置分片
func (s *Shard) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = s.items[:0]
	s.lastFlush = time.Now()
}
