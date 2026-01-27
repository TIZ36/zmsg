package id

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// PostgresStorage PostgreSQL存储实现
type PostgresStorage struct {
	db      *sql.DB
	ownedDB bool // 是否拥有 DB 连接（用于 Close 时判断是否关闭）
}

// NewPostgresStorage 创建PostgreSQL存储（创建新连接）
func NewPostgresStorage(dsn string) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// 配置连接池
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// 创建表（如果不存在）
	if err := createTables(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return &PostgresStorage{
		db:      db,
		ownedDB: true,
	}, nil
}

// NewPostgresStorageFromDB 从现有 DB 连接创建存储
func NewPostgresStorageFromDB(db *sql.DB) (*PostgresStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建表（如果不存在）
	if err := createTables(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return &PostgresStorage{
		db:      db,
		ownedDB: false, // 不拥有连接，Close 时不关闭
	}, nil
}

// createTables 创建表
func createTables(ctx context.Context, db *sql.DB) error {
	queries := []string{
		// 节点表
		`CREATE TABLE IF NOT EXISTS zmsg_nodes (
            id SERIAL PRIMARY KEY,
            node_id INTEGER NOT NULL,
            hostname VARCHAR(255) NOT NULL,
            ip VARCHAR(50) NOT NULL,
            service VARCHAR(255) NOT NULL,
            pid INTEGER NOT NULL,
            last_seen TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(node_id)
        )`,

		// 创建索引
		`CREATE INDEX IF NOT EXISTS idx_zmsg_nodes_expires ON zmsg_nodes(expires_at)`,
		`CREATE INDEX IF NOT EXISTS idx_zmsg_nodes_service ON zmsg_nodes(service)`,
		`CREATE INDEX IF NOT EXISTS idx_zmsg_nodes_last_seen ON zmsg_nodes(last_seen)`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute query: %w, query: %s", err, query)
		}
	}

	return nil
}

// RegisterNode 注册节点
func (s *PostgresStorage) RegisterNode(ctx context.Context, node *NodeInfo) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 首先清理过期节点
	if _, err := tx.ExecContext(ctx, `
        DELETE FROM zmsg_nodes WHERE expires_at < NOW()
    `); err != nil {
		return 0, fmt.Errorf("failed to cleanup expired nodes: %w", err)
	}

	maxNodeID := int64(1<<10 - 1) // 默认1024个节点

	// 查找可用的节点 ID（找到第一个空缺的 ID）
	var nodeID int64
	err = tx.QueryRowContext(ctx, `
        WITH used_ids AS (
            SELECT node_id FROM zmsg_nodes WHERE service = $1
        ),
        all_ids AS (
            SELECT generate_series(0, $2::bigint) AS id
        )
        SELECT COALESCE(MIN(all_ids.id), 0)
        FROM all_ids
        LEFT JOIN used_ids ON all_ids.id = used_ids.node_id
        WHERE used_ids.node_id IS NULL
    `, node.Service, maxNodeID).Scan(&nodeID)

	if err != nil {
		return 0, fmt.Errorf("failed to get available node id: %w", err)
	}

	// 检查是否找到可用的节点 ID
	if nodeID > maxNodeID {
		return 0, fmt.Errorf("no available node id, max=%d", maxNodeID)
	}

	// 插入或更新节点
	_, err = tx.ExecContext(ctx, `
        INSERT INTO zmsg_nodes (node_id, hostname, ip, service, pid, last_seen, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (node_id) DO UPDATE SET
            hostname = $2,
            ip = $3,
            service = $4,
            pid = $5,
            last_seen = $6,
            expires_at = $7
    `, nodeID, node.Hostname, node.IP, node.Service, node.PID,
		node.LastSeen, node.ExpiresAt)

	if err != nil {
		return 0, fmt.Errorf("failed to register node: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nodeID, nil
}

// RenewNode 续租节点
func (s *PostgresStorage) RenewNode(ctx context.Context, nodeID int64, ttl time.Duration) error {
	result, err := s.db.ExecContext(ctx, `
        UPDATE zmsg_nodes 
        SET last_seen = NOW(), expires_at = NOW() + $1 
        WHERE node_id = $2 AND expires_at > NOW()
    `, ttl, nodeID)

	if err != nil {
		return fmt.Errorf("failed to renew node: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("node not found or expired: %d", nodeID)
	}

	return nil
}

// GetActiveNodes 获取活跃节点
func (s *PostgresStorage) GetActiveNodes(ctx context.Context) ([]*NodeInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT node_id, hostname, ip, service, pid, last_seen, expires_at
        FROM zmsg_nodes 
        WHERE expires_at > NOW()
        ORDER BY node_id
    `)

	if err != nil {
		return nil, fmt.Errorf("failed to query active nodes: %w", err)
	}
	defer rows.Close()

	var nodes []*NodeInfo
	for rows.Next() {
		node := &NodeInfo{}
		err := rows.Scan(
			&node.ID,
			&node.Hostname,
			&node.IP,
			&node.Service,
			&node.PID,
			&node.LastSeen,
			&node.ExpiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan node: %w", err)
		}
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return nodes, nil
}

// CleanupExpiredNodes 清理过期节点
func (s *PostgresStorage) CleanupExpiredNodes(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
        DELETE FROM zmsg_nodes WHERE expires_at < NOW()
    `)

	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired nodes: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rows, nil
}

// GetNodeByID 根据ID获取节点
func (s *PostgresStorage) GetNodeByID(ctx context.Context, nodeID int64) (*NodeInfo, error) {
	node := &NodeInfo{}

	err := s.db.QueryRowContext(ctx, `
        SELECT node_id, hostname, ip, service, pid, last_seen, expires_at
        FROM zmsg_nodes 
        WHERE node_id = $1 AND expires_at > NOW()
    `, nodeID).Scan(
		&node.ID,
		&node.Hostname,
		&node.IP,
		&node.Service,
		&node.PID,
		&node.LastSeen,
		&node.ExpiresAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("node not found or expired: %d", nodeID)
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	return node, nil
}

// GetNodesByService 根据服务获取节点
func (s *PostgresStorage) GetNodesByService(ctx context.Context, service string) ([]*NodeInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT node_id, hostname, ip, service, pid, last_seen, expires_at
        FROM zmsg_nodes 
        WHERE service = $1 AND expires_at > NOW()
        ORDER BY node_id
    `, service)

	if err != nil {
		return nil, fmt.Errorf("failed to query nodes by service: %w", err)
	}
	defer rows.Close()

	var nodes []*NodeInfo
	for rows.Next() {
		node := &NodeInfo{}
		err := rows.Scan(
			&node.ID,
			&node.Hostname,
			&node.IP,
			&node.Service,
			&node.PID,
			&node.LastSeen,
			&node.ExpiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan node: %w", err)
		}
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return nodes, nil
}

// Stats 获取统计信息
func (s *PostgresStorage) Stats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 获取总节点数
	var totalNodes int64
	err := s.db.QueryRowContext(ctx, `
        SELECT COUNT(*) FROM zmsg_nodes WHERE expires_at > NOW()
    `).Scan(&totalNodes)

	if err != nil {
		return nil, fmt.Errorf("failed to get total nodes: %w", err)
	}
	stats["total_nodes"] = totalNodes

	// 获取按服务分组的节点数
	rows, err := s.db.QueryContext(ctx, `
        SELECT service, COUNT(*) 
        FROM zmsg_nodes 
        WHERE expires_at > NOW()
        GROUP BY service
    `)

	if err != nil {
		return nil, fmt.Errorf("failed to get nodes by service: %w", err)
	}
	defer rows.Close()

	serviceStats := make(map[string]int64)
	for rows.Next() {
		var service string
		var count int64
		if err := rows.Scan(&service, &count); err != nil {
			return nil, fmt.Errorf("failed to scan service stats: %w", err)
		}
		serviceStats[service] = count
	}
	stats["nodes_by_service"] = serviceStats

	// 获取即将过期的节点
	var expiringSoon int64
	err = s.db.QueryRowContext(ctx, `
        SELECT COUNT(*) 
        FROM zmsg_nodes 
        WHERE expires_at > NOW() AND expires_at < NOW() + INTERVAL '1 minute'
    `).Scan(&expiringSoon)

	if err != nil {
		return nil, fmt.Errorf("failed to get expiring nodes: %w", err)
	}
	stats["expiring_soon"] = expiringSoon

	return stats, nil
}

// Close 关闭存储
func (s *PostgresStorage) Close() error {
	if s.ownedDB {
		return s.db.Close()
	}
	return nil
}
