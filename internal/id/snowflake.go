package id

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Snowflake 雪花ID生成器
type Snowflake struct {
	mu        sync.Mutex
	epoch     int64
	nodeID    int64
	nodeBits  uint8
	stepBits  uint8
	stepMask  int64
	timeShift uint8
	nodeShift uint8
	lastTime  int64
	step      int64

	// 节点管理
	nodeMgr    *NodeManager
	autoNodeID bool

	// 统计
	stats *Stats
}

// Stats 统计信息
type Stats struct {
	Generated    int64
	LastGenerate time.Time
	AvgDuration  time.Duration
	Errors       int64
	NodeChanges  int64
}

// NewSnowflake 创建雪花ID生成器
func NewSnowflake(cfg *Config) (*Snowflake, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sf := &Snowflake{
		epoch:      cfg.Epoch,
		nodeBits:   cfg.NodeBits,
		stepBits:   cfg.StepBits,
		stepMask:   -1 ^ (-1 << cfg.StepBits),
		timeShift:  cfg.NodeBits + cfg.StepBits,
		nodeShift:  cfg.StepBits,
		autoNodeID: cfg.AutoNodeID,
		stats:      &Stats{},
	}

	// 初始化节点管理器
	if cfg.AutoNodeID {
		if cfg.Storage == nil {
			return nil, fmt.Errorf("storage is required for auto node id")
		}

		nodeMgr, err := NewNodeManager(cfg.Storage, cfg.Service, cfg.NodeTTL)
		if err != nil {
			return nil, err
		}
		sf.nodeMgr = nodeMgr

		// 获取节点ID（带超时，防止阻塞初始化）
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		nodeID, err := nodeMgr.AcquireNodeID(ctx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("failed to acquire node id: %w", err)
		}
		sf.nodeID = nodeID
	} else {
		// 使用配置的节点ID
		sf.nodeID = cfg.NodeID
		if sf.nodeID < 0 || sf.nodeID > (1<<cfg.NodeBits-1) {
			return nil, fmt.Errorf("node_id must be between 0 and %d", (1<<cfg.NodeBits)-1)
		}
	}

	return sf, nil
}

// Generate 生成ID
func (sf *Snowflake) Generate(ctx context.Context) (string, error) {
	start := time.Now()

	sf.mu.Lock()
	defer sf.mu.Unlock()

	now := time.Now().UnixMilli()

	// 检查时钟回拨
	if now < sf.lastTime {
		sf.stats.Errors++
		return "", fmt.Errorf("clock moved backwards, refusing to generate id")
	}

	// 同一毫秒内生成
	if now == sf.lastTime {
		sf.step = (sf.step + 1) & sf.stepMask
		if sf.step == 0 {
			// 序列号用尽，等待下一毫秒
			for now <= sf.lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		sf.step = 0
	}

	sf.lastTime = now

	// 生成ID
	id := ((now - sf.epoch) << sf.timeShift) |
		(sf.nodeID << sf.nodeShift) |
		sf.step

	// 更新统计
	sf.stats.Generated++
	sf.stats.LastGenerate = time.Now()
	duration := time.Since(start)
	if sf.stats.Generated > 1 {
		total := sf.stats.AvgDuration * time.Duration(sf.stats.Generated-1)
		sf.stats.AvgDuration = (total + duration) / time.Duration(sf.stats.Generated)
	} else {
		sf.stats.AvgDuration = duration
	}

	return fmt.Sprintf("%d", id), nil
}

// GenerateWithPrefix 生成带前缀的ID
func (sf *Snowflake) GenerateWithPrefix(ctx context.Context, prefix string) (string, error) {
	id, err := sf.Generate(ctx)
	if err != nil {
		return "", err
	}

	if prefix != "" {
		return fmt.Sprintf("%s_%s", prefix, id), nil
	}

	return id, nil
}

// Parse 解析ID
func (sf *Snowflake) Parse(id string) (int64, error) {
	var snowflakeID int64
	_, err := fmt.Sscanf(id, "%d", &snowflakeID)
	if err != nil {
		return 0, fmt.Errorf("invalid snowflake id: %w", err)
	}

	return snowflakeID, nil
}

// ExtractTime 提取时间
func (sf *Snowflake) ExtractTime(id string) (time.Time, error) {
	snowflakeID, err := sf.Parse(id)
	if err != nil {
		return time.Time{}, err
	}

	timestamp := (snowflakeID >> sf.timeShift) + sf.epoch
	return time.UnixMilli(timestamp), nil
}

// ExtractNodeID 提取节点ID
func (sf *Snowflake) ExtractNodeID(id string) (int64, error) {
	snowflakeID, err := sf.Parse(id)
	if err != nil {
		return 0, err
	}

	return (snowflakeID >> sf.nodeShift) & ((1 << sf.nodeBits) - 1), nil
}

// ExtractStep 提取序列号
func (sf *Snowflake) ExtractStep(id string) (int64, error) {
	snowflakeID, err := sf.Parse(id)
	if err != nil {
		return 0, err
	}

	return snowflakeID & sf.stepMask, nil
}

// GetNodeID 获取节点ID
func (sf *Snowflake) GetNodeID() int64 {
	return sf.nodeID
}

// Stats 获取统计信息
func (sf *Snowflake) Stats() map[string]interface{} {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	stats := make(map[string]interface{})
	stats["generated"] = sf.stats.Generated
	stats["last_generate"] = sf.stats.LastGenerate
	stats["avg_duration"] = sf.stats.AvgDuration
	stats["errors"] = sf.stats.Errors
	stats["node_id"] = sf.nodeID
	stats["node_changes"] = sf.stats.NodeChanges

	return stats
}

// Start 启动生成器
func (sf *Snowflake) Start(ctx context.Context) error {
	if sf.autoNodeID && sf.nodeMgr != nil {
		// 启动心跳协程
		go sf.nodeMgr.Heartbeat(ctx)

		// 启动节点监控
		go sf.monitorNode(ctx)
	}

	return nil
}

// monitorNode 监控节点状态
func (sf *Snowflake) monitorNode(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 检查节点是否仍然有效
			if sf.autoNodeID && sf.nodeMgr != nil {
				active, err := sf.nodeMgr.IsNodeActive(ctx, sf.nodeID)
				if err == nil && !active {
					// 节点失效，尝试重新获取
					sf.mu.Lock()
					newID, err := sf.nodeMgr.AcquireNodeID(ctx)
					if err == nil && newID != sf.nodeID {
						sf.nodeID = newID
						sf.stats.NodeChanges++
					}
					sf.mu.Unlock()
				}
			}
		}
	}
}

// Close 关闭生成器
func (sf *Snowflake) Close() error {
	if sf.nodeMgr != nil {
		return sf.nodeMgr.Close()
	}
	return nil
}

// NewDefaultSnowflake 创建默认的雪花ID生成器
func NewDefaultSnowflake(storage Storage) (*Snowflake, error) {
	cfg := DefaultConfig()
	cfg.Storage = storage

	return NewSnowflake(cfg)
}
