package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/tiz36/zmsg/core"
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
		cfg.Postgres.DSN = getenv("TEST_POSTGRES_DSN", "postgresql://test:test@localhost:5433/test?sslmode=disable")
		cfg.Redis.Addr = getenv("TEST_REDIS_ADDR", "localhost:6381")
		cfg.Redis.Password = getenv("TEST_REDIS_PASSWORD", "")
		cfg.Queue.Addr = getenv("TEST_QUEUE_ADDR", "localhost:6380")
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
		adminDSN := getenv("TEST_POSTGRES_ADMIN_DSN", "postgresql://test:test@localhost:5433/postgres?sslmode=disable")
		db, err := sql.Open("postgres", adminDSN)
		if err != nil {
			t.Logf("Warning: cannot connect to admin database: %v", err)
			t.Log("Please ensure the test database 'zmsg_test' exists")
			t.Log("Run: CREATE DATABASE zmsg_test;")
			return
		}
		defer db.Close()

		// 检查数据库是否存在
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var exists bool
		err = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = 'zmsg_test')").Scan(&exists)
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

		// 读取 schema.sql
		// 尝试多个路径以兼容不同的测试运行方式
		paths := []string{
			"sql/schema.sql",       // from tests/
			"tests/sql/schema.sql", // from root
			"../sql/schema.sql",    // from tests/testutil
		}
		var content []byte
		var err error
		for _, p := range paths {
			content, err = os.ReadFile(p)
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("failed to read schema.sql: %v", err)
		}

		err = zm.LoadSQL(string(content)).Migrate(ctx)
		if err != nil {
			t.Fatalf("failed to init schema: %v", err)
		}
	})
}

// CleanupTable 清理指定表的测试数据
func CleanupTable(t *testing.T, zm zmsg.ZMsg, table string, whereClause string, args ...interface{}) {
	_, err := zm.SQL("DELETE FROM "+table+" WHERE "+whereClause, args...).Exec()
	if err != nil {
		t.Logf("cleanup table %s failed: %v", table, err)
	}
}

// CleanupRedis 清理 Redis 数据 (包括 Asynq 队列)
func CleanupRedis(t *testing.T) {
	cfg := NewConfig()
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		if t != nil {
			t.Logf("flush redis failed: %v", err)
		} else {
			fmt.Printf("flush redis failed: %v\n", err)
		}
	}
}

// CleanupAllTables 清理所有测试表数据
func CleanupAllTables(t *testing.T, zm zmsg.ZMsg) {
	CleanupRedis(t) // 同时也清理 Redis

	paths := []string{
		"sql/clear.sql",       // from tests/
		"tests/sql/clear.sql", // from root
		"../sql/clear.sql",    // from tests/testutil
	}
	var content []byte
	var err error
	for _, p := range paths {
		content, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}
	if err != nil {
		if t != nil {
			t.Fatalf("failed to read clear.sql: %v", err)
		} else {
			panic(fmt.Sprintf("failed to read clear.sql: %v", err))
		}
	}

	_, err = zm.SQL(string(content)).Exec()
	if err != nil {
		if t != nil {
			t.Logf("cleanup all tables failed: %v", err)
		} else {
			fmt.Printf("cleanup all tables failed: %v\n", err)
		}
	}
}

// InsertTestFeed 插入测试 feed 数据
func InsertTestFeed(t *testing.T, zm zmsg.ZMsg, id, content string) {
	err := zm.Table("feeds").CacheKey(id).Save(struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}{ID: id, Content: content})
	if err != nil {
		t.Fatalf("insert test feed failed: %v", err)
	}
}

// InsertFeedMeta 插入 feed_meta 测试数据
func InsertFeedMeta(t *testing.T, zm zmsg.ZMsg, id string) {
	err := zm.Table("feed_meta").CacheKey("meta:" + id).Save(struct {
		ID string `db:"id,pk"`
	}{ID: id})
	if err != nil {
		t.Fatalf("insert feed_meta failed: %v", err)
	}
}

// InsertFeedReplyMeta 插入 feed_reply_meta 测试数据
func InsertFeedReplyMeta(t *testing.T, zm zmsg.ZMsg, id, metaType string) {
	err := zm.Table("feed_reply_meta").CacheKey("meta:" + id).Save(struct {
		ID   string `db:"id,pk"`
		Type string `db:"type"`
	}{ID: id, Type: metaType})
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
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
