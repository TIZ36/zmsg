package id

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// NodeManager 节点管理器
type NodeManager struct {
	storage Storage
	service string
	ttl     time.Duration

	nodeID   int64
	hostname string
	ip       string
	pid      int

	mu     sync.RWMutex
	active bool
	stopCh chan struct{}
	wg     sync.WaitGroup

	// 缓存
	nodeCache map[int64]*NodeInfo
	cacheMu   sync.RWMutex
}

// NewNodeManager 创建节点管理器
func NewNodeManager(storage Storage, service string, ttl time.Duration) (*NodeManager, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	// 获取主机信息
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	ip := getLocalIP()
	pid := os.Getpid()

	mgr := &NodeManager{
		storage:   storage,
		service:   service,
		ttl:       ttl,
		hostname:  hostname,
		ip:        ip,
		pid:       pid,
		stopCh:    make(chan struct{}),
		nodeCache: make(map[int64]*NodeInfo),
	}

	return mgr, nil
}

// AcquireNodeID 获取节点ID
func (m *NodeManager) AcquireNodeID(ctx context.Context) (int64, error) {
	nodeInfo := &NodeInfo{
		Hostname:  m.hostname,
		IP:        m.ip,
		Service:   m.service,
		PID:       m.pid,
		LastSeen:  time.Now(),
		ExpiresAt: time.Now().Add(m.ttl),
	}

	// 尝试注册节点
	nodeID, err := m.storage.RegisterNode(ctx, nodeInfo)
	if err != nil {
		return 0, fmt.Errorf("failed to register node: %w", err)
	}

	m.mu.Lock()
	m.nodeID = nodeID
	m.active = true
	m.mu.Unlock()

	// 缓存节点信息
	m.cacheMu.Lock()
	m.nodeCache[nodeID] = nodeInfo
	m.cacheMu.Unlock()

	return nodeID, nil
}

// RenewNode 续租节点
func (m *NodeManager) RenewNode(ctx context.Context) error {
	m.mu.RLock()
	nodeID := m.nodeID
	m.mu.RUnlock()

	if nodeID == 0 {
		return fmt.Errorf("node not acquired")
	}

	err := m.storage.RenewNode(ctx, nodeID, m.ttl)
	if err != nil {
		m.mu.Lock()
		m.active = false
		m.mu.Unlock()
		return fmt.Errorf("failed to renew node: %w", err)
	}

	// 更新缓存
	m.cacheMu.Lock()
	if node, exists := m.nodeCache[nodeID]; exists {
		node.LastSeen = time.Now()
		node.ExpiresAt = time.Now().Add(m.ttl)
	}
	m.cacheMu.Unlock()

	return nil
}

// IsNodeActive 检查节点是否活跃
func (m *NodeManager) IsNodeActive(ctx context.Context, nodeID int64) (bool, error) {
	// 先检查缓存
	m.cacheMu.RLock()
	node, exists := m.nodeCache[nodeID]
	m.cacheMu.RUnlock()

	if exists && node.ExpiresAt.After(time.Now()) {
		return true, nil
	}

	// 从存储中获取最新信息
	nodes, err := m.storage.GetActiveNodes(ctx)
	if err != nil {
		return false, err
	}

	// 更新缓存
	m.cacheMu.Lock()
	m.nodeCache = make(map[int64]*NodeInfo)
	for _, n := range nodes {
		m.nodeCache[n.ID] = n
		if n.ID == nodeID && n.ExpiresAt.After(time.Now()) {
			m.cacheMu.Unlock()
			return true, nil
		}
	}
	m.cacheMu.Unlock()

	return false, nil
}

// GetActiveNodes 获取活跃节点
func (m *NodeManager) GetActiveNodes(ctx context.Context) ([]*NodeInfo, error) {
	nodes, err := m.storage.GetActiveNodes(ctx)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	m.cacheMu.Lock()
	m.nodeCache = make(map[int64]*NodeInfo)
	for _, node := range nodes {
		m.nodeCache[node.ID] = node
	}
	m.cacheMu.Unlock()

	return nodes, nil
}

// Heartbeat 心跳协程
func (m *NodeManager) Heartbeat(ctx context.Context) {
	m.wg.Add(1)
	defer m.wg.Done()

	ticker := time.NewTicker(m.ttl / 2) // 每TTL的一半续租一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			if err := m.RenewNode(ctx); err != nil {
				// 重试逻辑
				for i := 0; i < 3; i++ {
					time.Sleep(time.Second * time.Duration(i+1))
					if err := m.RenewNode(ctx); err == nil {
						break
					}
				}
			}
		}
	}
}

// Cleanup 清理过期节点
func (m *NodeManager) Cleanup(ctx context.Context) (int64, error) {
	return m.storage.CleanupExpiredNodes(ctx)
}

// GetNodeID 获取当前节点ID
func (m *NodeManager) GetNodeID() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodeID
}

// GetNodeInfo 获取节点信息
func (m *NodeManager) GetNodeInfo() *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &NodeInfo{
		ID:        m.nodeID,
		Hostname:  m.hostname,
		IP:        m.ip,
		Service:   m.service,
		PID:       m.pid,
		LastSeen:  time.Now(),
		ExpiresAt: time.Now().Add(m.ttl),
	}
}

// Stats 获取统计信息
func (m *NodeManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()

	stats := make(map[string]interface{})
	stats["node_id"] = m.nodeID
	stats["active"] = m.active
	stats["hostname"] = m.hostname
	stats["ip"] = m.ip
	stats["service"] = m.service
	stats["pid"] = m.pid
	stats["cached_nodes"] = len(m.nodeCache)

	return stats
}

// Close 关闭节点管理器
func (m *NodeManager) Close() error {
	close(m.stopCh)
	m.wg.Wait()
	return nil
}

// getLocalIP 获取本地IP
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return ""
}
