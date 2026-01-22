package id

import (
	"context"
	"fmt"
	"time"
)

// Generator ID生成器接口
type Generator interface {
	// Generate 生成ID
	Generate(ctx context.Context) (string, error)

	// GenerateWithPrefix 生成带前缀的ID
	GenerateWithPrefix(ctx context.Context, prefix string) (string, error)

	// Parse 解析ID
	Parse(id string) (int64, error)

	// GetNodeID 获取节点ID
	GetNodeID() int64

	// Stats 获取统计信息
	Stats() map[string]interface{}

	// Close 关闭生成器
	Close() error
}

// Config ID生成器配置
type Config struct {
	// 基础配置
	Epoch    int64 // 开始时间戳（毫秒）
	NodeBits uint8 // 节点ID位数
	StepBits uint8 // 序列号位数
	NodeID   int64 // 节点ID（如果为0则自动获取）

	// 节点管理配置
	AutoNodeID bool          // 是否自动管理节点ID
	NodeTTL    time.Duration // 节点租约TTL
	Service    string        // 服务名称

	// 存储配置
	Storage Storage // 存储接口
}

// Storage 存储接口（用于节点ID管理）
type Storage interface {
	// 注册节点
	RegisterNode(ctx context.Context, node *NodeInfo) (int64, error)
	// 续租节点
	RenewNode(ctx context.Context, nodeID int64, ttl time.Duration) error
	// 获取活跃节点
	GetActiveNodes(ctx context.Context) ([]*NodeInfo, error)
	// 清理过期节点
	CleanupExpiredNodes(ctx context.Context) (int64, error)
}

// NodeInfo 节点信息
type NodeInfo struct {
	ID        int64
	Hostname  string
	IP        string
	Service   string
	PID       int
	LastSeen  time.Time
	ExpiresAt time.Time
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Epoch:      1609459200000, // 2021-01-01 00:00:00 UTC
		NodeBits:   10,            // 1024个节点
		StepBits:   12,            // 4096个序列号/毫秒
		AutoNodeID: true,
		NodeTTL:    30 * time.Second,
		Service:    "zmsg",
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.NodeBits+c.StepBits > 22 { // 时间戳占41位
		return fmt.Errorf("node_bits + step_bits must be <= 22")
	}

	if c.Epoch <= 0 {
		return fmt.Errorf("epoch must be positive")
	}

	if c.NodeTTL <= 0 {
		return fmt.Errorf("node_ttl must be positive")
	}

	return nil
}
