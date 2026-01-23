package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	"github.com/tiz36/zmsg/zmsg"
)

var (
	testConfig     zmsg.Config
	testConfigOnce sync.Once
	schemaOnce     sync.Once
	dbInitOnce     sync.Once
)

// InitEnv 初始化测试环境配置
func InitEnv() {
	testConfigOnce.Do(func() {
		cfg := zmsg.DefaultConfig()
		// 使用环境变量或默认值
		cfg.Postgres.DSN = getenv("TEST_POSTGRES_DSN", "postgresql://postgres:postgres@localhost/zmsg_test?sslmode=disable")
		cfg.Redis.Addr = getenv("TEST_REDIS_ADDR", "localhost:6379")
		cfg.Redis.Password = getenv("TEST_REDIS_PASSWORD", "123456")
		cfg.Queue.Addr = getenv("TEST_QUEUE_ADDR", cfg.Redis.Addr)
		cfg.Queue.Password = cfg.Redis.Password
		cfg.Log.Level = "warn" // 测试时减少日志
		testConfig = cfg
	})
}

// NewConfig 获取测试配置
func NewConfig() zmsg.Config {
	InitEnv()
	return testConfig
}

// EnsureTestDatabase 确保测试数据库存在
func EnsureTestDatabase(t *testing.T) {
	dbInitOnce.Do(func() {
		// 连接到 postgres 默认数据库来创建测试数据库
		adminDSN := getenv("TEST_POSTGRES_ADMIN_DSN", "postgresql://postgres:postgres@localhost/postgres?sslmode=disable")
		db, err := sql.Open("postgres", adminDSN)
		if err != nil {
			t.Logf("Warning: cannot connect to admin database: %v", err)
			t.Log("Please ensure the test database 'zmsg_test' exists")
			t.Log("Run: CREATE DATABASE zmsg_test;")
			return
		}
		defer db.Close()

		// 检查数据库是否存在
		var exists bool
		err = db.QueryRow("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = 'zmsg_test')").Scan(&exists)
		if err != nil {
			t.Logf("Warning: cannot check database existence: %v", err)
			return
		}

		if !exists {
			// 创建测试数据库
			_, err = db.Exec("CREATE DATABASE zmsg_test")
			if err != nil {
				t.Logf("Warning: cannot create test database: %v", err)
				t.Log("Please manually run: CREATE DATABASE zmsg_test;")
			} else {
				t.Log("Created test database: zmsg_test")
			}
		}
	})
}

// NewZmsg 创建 zmsg 实例
func NewZmsg(t *testing.T) zmsg.ZMsg {
	EnsureTestDatabase(t)

	cfg := NewConfig()
	ctx := context.Background()
	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create zmsg instance: %v", err)
	}
	return zm
}

// NewZmsgWithSchema 创建 zmsg 实例并初始化测试表
func NewZmsgWithSchema(t *testing.T) zmsg.ZMsg {
	zm := NewZmsg(t)
	InitSchema(t, zm)
	return zm
}

// InitSchema 初始化测试数据库表结构
func InitSchema(t *testing.T, zm zmsg.ZMsg) {
	schemaOnce.Do(func() {
		ctx := context.Background()

		// 创建测试表
		schema := `
-- 测试用 feeds 表
CREATE TABLE IF NOT EXISTS test_feeds (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    content TEXT,
    like_count INTEGER DEFAULT 0,
    repost_count INTEGER DEFAULT 0,
    status VARCHAR(32) DEFAULT 'active',
    tags JSONB DEFAULT '[]'::jsonb,
    extra JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 测试用 feed_meta 表（计数器聚合测试）
CREATE TABLE IF NOT EXISTS feed_meta (
    id VARCHAR(64) PRIMARY KEY,
    like_count INTEGER DEFAULT 0,
    repost_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    score NUMERIC DEFAULT 0,
    tags JSONB DEFAULT '[]'::jsonb,
    extra JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 测试用 feed_reply_meta 表
CREATE TABLE IF NOT EXISTS feed_reply_meta (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) DEFAULT 'feed',
    visibility VARCHAR(32) DEFAULT 'public',
    topic VARCHAR(255),
    is_repostable BOOLEAN DEFAULT true,
    is_original BOOLEAN DEFAULT true,
    like_count INTEGER DEFAULT 0,
    repost_count INTEGER DEFAULT 0,
    tags JSONB DEFAULT '[]'::jsonb,
    extra JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 测试用 replies 表
CREATE TABLE IF NOT EXISTS replies (
    id VARCHAR(64) PRIMARY KEY,
    feed_id VARCHAR(64),
    user_id VARCHAR(64),
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 测试用 likes 表（唯一约束测试）
CREATE TABLE IF NOT EXISTS likes (
    user_id VARCHAR(64) NOT NULL,
    feed_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, feed_id)
);

-- 测试用 reply_likes 表
CREATE TABLE IF NOT EXISTS reply_likes (
    reply_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (reply_id, user_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_test_feeds_user_id ON test_feeds(user_id);
CREATE INDEX IF NOT EXISTS idx_test_feeds_status ON test_feeds(status);
CREATE INDEX IF NOT EXISTS idx_replies_feed_id ON replies(feed_id);
`
		err := zm.LoadSQL(schema).Migrate(ctx)
		if err != nil {
			t.Fatalf("failed to init schema: %v", err)
		}
	})
}

// CleanupTable 清理指定表的测试数据
func CleanupTable(t *testing.T, zm zmsg.ZMsg, table string, whereClause string, args ...interface{}) {
	ctx := context.Background()
	task := zmsg.SQL("DELETE FROM "+table+" WHERE "+whereClause, args...)
	_, err := zm.SQLExec(ctx, task)
	if err != nil {
		t.Logf("cleanup table %s failed: %v", table, err)
	}
}

// CleanupAllTables 清理所有测试表数据
func CleanupAllTables(t *testing.T, zm zmsg.ZMsg) {
	ctx := context.Background()
	tables := []string{
		"test_feeds",
		"feed_meta",
		"feed_reply_meta",
		"replies",
		"likes",
		"reply_likes",
	}
	for _, table := range tables {
		task := zmsg.SQL("TRUNCATE TABLE " + table + " CASCADE")
		_, err := zm.SQLExec(ctx, task)
		if err != nil {
			t.Logf("truncate table %s failed: %v", table, err)
		}
	}
}

// InsertTestFeed 插入测试 feed 数据
func InsertTestFeed(t *testing.T, zm zmsg.ZMsg, id, content string) {
	ctx := context.Background()
	task := zmsg.SQL("INSERT INTO test_feeds (id, content) VALUES (?, ?)", id, content).
		OnConflict("id").
		DoUpdate("content")
	_, err := zm.SQLExec(ctx, task)
	if err != nil {
		t.Fatalf("insert test feed failed: %v", err)
	}
}

// InsertFeedMeta 插入 feed_meta 测试数据
func InsertFeedMeta(t *testing.T, zm zmsg.ZMsg, id string) {
	ctx := context.Background()
	task := zmsg.SQL("INSERT INTO feed_meta (id) VALUES (?)", id).
		OnConflict("id").
		DoNothing()
	_, err := zm.SQLExec(ctx, task)
	if err != nil {
		t.Fatalf("insert feed_meta failed: %v", err)
	}
}

// InsertFeedReplyMeta 插入 feed_reply_meta 测试数据
func InsertFeedReplyMeta(t *testing.T, zm zmsg.ZMsg, id, metaType string) {
	ctx := context.Background()
	task := zmsg.SQL("INSERT INTO feed_reply_meta (id, type) VALUES (?, ?)", id, metaType).
		OnConflict("id").
		DoNothing()
	_, err := zm.SQLExec(ctx, task)
	if err != nil {
		t.Fatalf("insert feed_reply_meta failed: %v", err)
	}
}

// SkipIfNoDatabase 如果数据库不可用则跳过测试
func SkipIfNoDatabase(t *testing.T) {
	cfg := NewConfig()
	db, err := sql.Open("postgres", cfg.Postgres.DSN)
	if err != nil {
		t.Skipf("skipping test: cannot connect to database: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("skipping test: database not available: %v", err)
	}
}

// PrintTestInfo 打印测试环境信息
func PrintTestInfo(t *testing.T) {
	cfg := NewConfig()
	t.Logf("Test environment:")
	t.Logf("  PostgreSQL: %s", maskPassword(cfg.Postgres.DSN))
	t.Logf("  Redis: %s", cfg.Redis.Addr)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func maskPassword(dsn string) string {
	// 简单的密码掩码处理
	return fmt.Sprintf("%s...", dsn[:30])
}
