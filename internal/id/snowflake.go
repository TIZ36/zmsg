package id

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	epoch     = 1609459200000 // 2021-01-01 00:00:00 UTC
	timeBits  = 41
	nodeBits  = 10
	seqBits   = 12
	nodeMax   = 1 << nodeBits  // 1024
	seqMask   = 1<<seqBits - 1 // 4095
	timeShift = nodeBits + seqBits
	nodeShift = seqBits
)

// snowflakeGenerator 雪花ID生成器
type snowflakeGenerator struct {
	mu       sync.Mutex
	nodeID   int64
	lastTime int64
	sequence int64

	db    *sql.DB
	redis *redis.Client
	ttl   time.Duration
}

// newSnowflakeGenerator 创建雪花ID生成器
func newSnowflakeGenerator(db *sql.DB, redis *redis.Client, ttl time.Duration) *snowflakeGenerator {
	return &snowflakeGenerator{
		db:    db,
		redis: redis,
		ttl:   ttl,
	}
}

// Generate 生成ID
func (g *snowflakeGenerator) Generate() (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 确保有节点ID
	if g.nodeID == 0 {
		if err := g.acquireNodeID(context.Background()); err != nil {
			return 0, err
		}
	}

	now := time.Now().UnixMilli()

	if now < g.lastTime {
		return 0, fmt.Errorf("clock moved backwards")
	}

	if now == g.lastTime {
		g.sequence = (g.sequence + 1) & seqMask
		if g.sequence == 0 {
			// 序列号用尽，等待下一毫秒
			for now <= g.lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTime = now

	id := (now-epoch)<<timeShift | (g.nodeID << nodeShift) | g.sequence
	return id, nil
}

// acquireNodeID 获取节点ID
func (g *snowflakeGenerator) acquireNodeID(ctx context.Context) error {
	// 尝试从 Redis 获取
	if nodeID, err := g.redis.Get(ctx, "zmsg:node:id").Int64(); err == nil && nodeID > 0 {
		g.nodeID = nodeID
		return nil
	}

	// 从数据库分配
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	hostname, _ := os.Hostname()
	ip := getLocalIP()

	var nodeID int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO zmsg_nodes (node_id, hostname, ip, expires_at)
        SELECT COALESCE(MAX(node_id), 0) + 1, $1, $2, NOW() + $3
        FROM zmsg_nodes
        WHERE expires_at > NOW()
        RETURNING node_id
    `, hostname, ip, g.ttl).Scan(&nodeID)

	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	g.nodeID = nodeID

	// 缓存到 Redis
	g.redis.Set(ctx, "zmsg:node:id", nodeID, g.ttl/2)

	return nil
}

// heartbeat 心跳协程，续租节点
func (g *snowflakeGenerator) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(g.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.renewNode(ctx)
		}
	}
}

// renewNode 续租节点
func (g *snowflakeGenerator) renewNode(ctx context.Context) {
	if g.nodeID == 0 {
		return
	}

	_, err := g.db.ExecContext(ctx, `
        UPDATE zmsg_nodes 
        SET expires_at = NOW() + $1 
        WHERE node_id = $2 AND expires_at > NOW()
    `, g.ttl, g.nodeID)

	if err == nil {
		// 更新 Redis 缓存
		g.redis.Expire(ctx, "zmsg:node:id", g.ttl/2)
	}
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
