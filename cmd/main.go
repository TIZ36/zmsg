package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	"github.com/tiz36/zmsg/core"
)

// ========== 1. 定义业务模型 ==========

type User struct {
	ID        string    `db:"id,pk"`
	Name      string    `db:"name"`
	Balance   int64     `db:"balance"` // 用于演示分布式序列化转账
	Meta      string    `db:"meta"`    // JSONB，用于演示 Deep Merge
	CreatedAt time.Time `db:"created_at"`
}

type Thread struct {
	ID        string    `db:"id,pk"`
	AuthorID  string    `db:"author_id"`
	Content   string    `db:"content"`
	LikeCount int64     `db:"like_count"` // 用于演示聚合计数
	Tags      string    `db:"tags"`       // JSONB list
	CreatedAt time.Time `db:"created_at"`
}

type Like struct {
	UserID    string    `db:"user_id,pk"`
	ThreadID  string    `db:"thread_id,pk"`
	CreatedAt time.Time `db:"created_at"`
}

// ========== 2. 模拟业务流程 ==========

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 初始化
	cfg, err := zmsg.LoadConfig("./config.yaml")
	if err != nil {
		// Fallback to default if file not found
		fmt.Println("Config file not found, using defaults...")
		cfg = zmsg.DefaultConfig()
		cfg.Postgres.DSN = "postgresql://postgres:postgres@localhost:5432/zmsg?sslmode=disable"
		cfg.Redis.Addr = "localhost:6379"
		cfg.Redis.Password = "chatee_redis" // 假设密码
		cfg.Queue.Addr = "localhost:6379"
		cfg.Queue.Password = "chatee_redis"
	}

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize zmsg: %v", err)
	}
	defer zm.Close()

	fmt.Println("✓ zmsg initialized")

	// 2. 初始化 Schema (模拟复杂环境)
	initSchema(ctx, zm)

	// 3. 启动 Asynq 监控
	go startAsynqMonitor(cfg)

	// 4. 运行模拟场景
	runScenario(ctx, zm)

	// 5. 等待退出
	fmt.Println("\n✓ Simulation running. Press Ctrl+C to exit...")
	fmt.Println("  Asynq Monitor: http://localhost:8080")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n✓ Shutting down...")
}

func initSchema(ctx context.Context, zm zmsg.ZMsg) {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT,
		balance BIGINT DEFAULT 0,
		meta JSONB DEFAULT '{}',
		created_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS threads (
		id TEXT PRIMARY KEY,
		author_id TEXT,
		content TEXT,
		like_count BIGINT DEFAULT 0,
		tags JSONB DEFAULT '[]',
		created_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS likes (
		user_id TEXT,
		thread_id TEXT,
		created_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY (user_id, thread_id)
	);
	CREATE TABLE IF NOT EXISTS zmsg_data (
		id TEXT PRIMARY KEY,
		data BYTEA,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);
	`
	if err := zm.LoadSQL(schema).Migrate(ctx); err != nil {
		log.Fatalf("Schema migration failed: %v", err)
	}
	fmt.Println("✓ Schema initialized")
}

func runScenario(ctx context.Context, zm zmsg.ZMsg) {
	// 场景 A: 创建用户 (同步写)
	fmt.Println("\n--- [A] Creating Users (Sync) ---")
	users := make([]*User, 5)
	for i := 0; i < 5; i++ {
		uid, _ := zm.NextID(ctx, "user")
		user := &User{
			ID:        uid,
			Name:      fmt.Sprintf("User_%d", i),
			Balance:   1000,
			CreatedAt: time.Now(),
		}
		if err := zm.Table("users").CacheKey(uid).Save(user); err != nil {
			log.Printf("Failed to create user: %v", err)
		}
		users[i] = user
		fmt.Printf("Created User: %s (Balance: %d)\n", uid, user.Balance)
	}

	// 场景 B: 发布帖子 (异步/周期写) -> 提高吞吐
	fmt.Println("\n--- [B] Creating Threads (Periodic/Override) ---")
	threadID, _ := zm.NextID(ctx, "thread")
	thread := &Thread{
		ID:        threadID,
		AuthorID:  users[0].ID,
		Content:   "Hello zmsg world! High performance storage.",
		LikeCount: 0,
		CreatedAt: time.Now(),
	}
	// 使用 PeriodicOverride，适合高频发帖场景，后台批量落库
	if err := zm.Table("threads").CacheKey(threadID).PeriodicOverride().Save(thread); err != nil {
		log.Printf("Failed to create thread: %v", err)
	}
	fmt.Printf("Created Thread: %s\n", threadID)

	// 场景 C: 高并发点赞 (内存聚合计数)
	fmt.Println("\n--- [C] High Concurrency Likes (Aggregation) ---")
	var wg sync.WaitGroup
	start := time.Now()
	likeCount := 1000

	for i := 0; i < likeCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 聚合计数：1000次点击只会产生极少量的 DB UPDATE
			err := zm.Table("threads").
				CacheKey(threadID). // L1/L2 缓存 key
				PeriodicCount(). // 启用计数器聚合策略
				UpdateColumn().Column("like_count").
				Do(zmsg.Add(), 1)
			if err != nil {
				log.Printf("Like failed: %v", err)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Processed %d likes in %v (Requests merged in memory)\n", likeCount, time.Since(start))

	// 场景 D: 用户信息局部更新 (Deep Merge)
	fmt.Println("\n--- [D] Partial Updates (Deep Merge) ---")
	targetUser := users[0].ID
	// 并发更新同一个 JSON 字段的不同属性
	wg.Add(2)
	go func() {
		defer wg.Done()
		// 更新 meta.theme
		_ = zm.Table("users").CacheKey(targetUser).PeriodicMerge().
			UpdateColumns(map[string]any{"meta": map[string]any{"theme": "dark"}})
	}()
	go func() {
		defer wg.Done()
		// 更新 meta.lang
		_ = zm.Table("users").CacheKey(targetUser).PeriodicMerge().
			UpdateColumns(map[string]any{"meta": map[string]any{"lang": "zh-CN"}})
	}()
	wg.Wait()
	fmt.Println("Merged user meta updates (theme=dark, lang=zh-CN)")

	// 场景 E: 转账 (分布式锁/序列化)
	fmt.Println("\n--- [E] Balance Transfer (Distributed Serialization) ---")
	// 即使并发执行，也能保证余额正确扣减，不会出现竞态
	// SerializeKey 确保同一用户的操作串行化
	balanceOps := 10
	var successCount int32
	for i := 0; i < balanceOps; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := zm.Table("users").
				CacheKey(targetUser).
				Serialize(targetUser). // 关键：分布式锁键
				UpdateColumn().Column("balance").
				Do(zmsg.Add(), -10) // 扣 10 块
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Executed %d balance deductions safely\n", atomic.LoadInt32(&successCount))

	// 场景 F: 列表查询 (QueryRow Slice Scan)
	fmt.Println("\n--- [F] List Query (Smart Slice Scan) ---")
	time.Sleep(1 * time.Second) // 等待部分异步写入完成

	var threadList []*Thread
	// 查询该作者的所有帖子
	query := "SELECT id, author_id, content, like_count, created_at FROM threads WHERE author_id = ? ORDER BY created_at DESC"

	// zmsg 自动识别 []*Thread 并执行 Query + Scan
	err := zm.SQL(query, users[0].ID).QueryRow(&threadList)
	if err != nil {
		log.Printf("Query list failed: %v", err)
	} else {
		fmt.Printf("Found %d threads for user %s\n", len(threadList), users[0].Name)
		for _, t := range threadList {
			// 注意：由于是异步写入，立即查询可能查不到最新的 like_count（最终一致性）
			// 但 ID 和 Content (PeriodicOverride) 通常较快
			fmt.Printf(" - [%s] %s (Likes: %d)\n", t.ID, t.Content, t.LikeCount)
		}
	}

	// 场景 G: 复杂联表查询 (LEFT JOIN + 外部参数)
	fmt.Println("\n--- [G] Complex JOIN Query (LEFT JOIN with Parameters) ---")

	// 定义联表结果结构
	type ThreadWithAuthor struct {
		ThreadID      string    `db:"thread_id"`
		Content       string    `db:"content"`
		LikeCount     int64     `db:"like_count"`
		AuthorID      string    `db:"author_id"`
		AuthorName    string    `db:"author_name"`
		AuthorBalance int64     `db:"author_balance"`
		CreatedAt     time.Time `db:"created_at"`
	}

	// 使用外部变量进行过滤
	minLikes := int64(0)        // 最小点赞数
	authorFilter := users[0].ID // 指定作者
	limit := 10

	var joinResults []*ThreadWithAuthor
	joinQuery := `
		SELECT 
			t.id as thread_id,
			t.content,
			t.like_count,
			t.author_id,
			u.name as author_name,
			u.balance as author_balance,
			t.created_at
		FROM threads t
		LEFT JOIN users u ON t.author_id = u.id
		WHERE t.like_count >= ? AND t.author_id = ?
		ORDER BY t.created_at DESC
		LIMIT ?
	`

	// 传递外部参数到 SQL 查询
	err = zm.SQL(joinQuery, minLikes, authorFilter, limit).QueryRow(&joinResults)
	if err != nil {
		log.Printf("JOIN query failed: %v", err)
	} else {
		fmt.Printf("Found %d threads (likes >= %d) by author %s:\n",
			len(joinResults), minLikes, users[0].Name)
		for _, r := range joinResults {
			fmt.Printf(" - Thread[%s] by %s (Balance: %d): %s (Likes: %d)\n",
				r.ThreadID[:8], r.AuthorName, r.AuthorBalance, r.Content, r.LikeCount)
		}
	}
}

// startAsynqMonitor 启动 Asynq 监控界面
func startAsynqMonitor(cfg zmsg.Config) {
	h := asynqmon.New(asynqmon.Options{
		RootPath:     "/",
		RedisConnOpt: asynq.RedisClientOpt{Addr: cfg.Queue.Addr, Password: cfg.Queue.Password, DB: cfg.Queue.DB},
	})

	srv := &http.Server{Addr: ":8080", Handler: h}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Asynq monitor error: %v", err)
		}
	}()
}

func demonstrateSqlExec(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	zm.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello").Exec()
}

func demonstrateNormalConcurrent(ctx context.Context, zm zmsg.ZMsg) {
	var wg sync.WaitGroup
	count := 20
	fmt.Printf("  - Saving %d unique feeds concurrently...\n", count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id, _ := zm.NextID(ctx, "normal")
			feed := Feed{
				ID:      id,
				UserID:  int64(100 + idx),
				Content: fmt.Sprintf("Normal content %d", idx),
				Status:  "active",
			}
			if err := zm.Table("feeds").CacheKey(id).Save(feed); err != nil {
				log.Printf("Failed to save: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

func demonstrateCounterConcurrent(ctx context.Context, zm zmsg.ZMsg) string {
	feedID, _ := zm.NextID(ctx, "feed")
	// 初始化记录
	_ = zm.Table("feed_reply_meta").Save(FeedReplyMeta{
		ID: feedID, Type: "feed", LikeCount: 0,
	})

	var wg sync.WaitGroup
	count := 200
	fmt.Printf("  - Incrementing likes %d times on feed %s...\n", count, feedID)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = zm.Table("feed_reply_meta").
				CacheKey("meta:"+feedID).
				Where("id = ?", feedID).
				PeriodicCount().
				UpdateColumn().
				Column("like_count").
				Do(zmsg.Add(), 1)
		}()
	}
	wg.Wait()
	return feedID
}

func demonstrateMergeConcurrent(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	var wg sync.WaitGroup
	count := 50
	fmt.Printf("  - Merging content %d times for feed %s...\n", count, feedID)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = zm.Table("feeds").
				CacheKey(feedID).
				Where("id = ?", feedID).
				PeriodicMerge().
				UpdateColumns(map[string]any{
					"content": fmt.Sprintf("Merged %d", idx),
				})
		}(i)
	}
	wg.Wait()
}

func demonstrateOverrideConcurrent(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	var wg sync.WaitGroup
	count := 50
	fmt.Printf("  - Overriding feed %s %d times...\n", feedID, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = zm.Table("feeds").
				CacheKey(feedID).
				PeriodicOverride().
				Save(Feed{
					ID:      feedID,
					UserID:  999,
					Content: fmt.Sprintf("Override %d", idx),
					Status:  "updated",
				})
		}(i)
	}
	wg.Wait()
}

func demonstrateDistributedConcurrent(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	var wg sync.WaitGroup
	count := 10
	fmt.Printf("  - Distributed sequence for feed %s (%d replies)...\n", feedID, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			replyID, _ := zm.NextID(ctx, "reply")
			reply := Reply{
				ID:      replyID,
				FeedID:  feedID,
				UserID:  "user_1",
				Content: fmt.Sprintf("Reply %d", idx),
			}
			// 使用 Serialize 确保对同一 feed 的回复按提交顺序处理
			_ = zm.Table("replies").
				CacheKey("reply:" + replyID).
				Serialize("feed_replies:" + feedID).
				Save(reply)
		}(i)
	}
	wg.Wait()
}

// Feed 数据结构
type Feed struct {
	ID        string    `json:"id" db:"id,pk"`
	UserID    int64     `json:"user_id" db:"user_id"`
	Content   string    `json:"content" db:"content"`
	LikeCount int64     `json:"like_count" db:"like_count"`
	Status    string    `json:"status" db:"status"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// Reply 回复数据结构
type Reply struct {
	ID        string    `json:"id" db:"id,pk"`
	FeedID    string    `json:"feed_id" db:"feed_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	ParentID  string    `json:"parent_id,omitempty" db:"parent_id"`
	Content   string    `json:"content" db:"content"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// FeedReplyMeta 元数据结构
type FeedReplyMeta struct {
	ID           string `json:"id" db:"id,pk"`
	Type         string `json:"type" db:"type"` // "feed" 或 "reply"
	Visibility   string `json:"visibility" db:"visibility"`
	Topic        string `json:"topic,omitempty" db:"topic"`
	IsRepostable bool   `json:"is_repostable" db:"is_repostable"`
	IsOriginal   bool   `json:"is_original" db:"is_original"`
	LikeCount    int64  `json:"like_count" db:"like_count"`
	RepostCount  int64  `json:"repost_count" db:"repost_count"`
}
