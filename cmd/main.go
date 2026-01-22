package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/tiz36/zmsg/zmsg"
)

func main() {
	ctx := context.Background()

	// ========== 1. 加载配置，初始化 zmsg ==========
	cfg, err := zmsg.LoadConfig("cmd/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize zmsg: %v", err)
	}
	defer zm.Close()

	fmt.Println("✓ zmsg initialized")

	// ========== 2. 加载 SQL 并执行增量迁移 ==========
	if err := zm.LoadDir("cmd/schema").Migrate(ctx); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}
	fmt.Println("✓ migrations completed")

	// ========== 3. 业务操作示例 ==========

	// 3.1 使用 Builder（适合复杂 SQL）
	fmt.Println("\n--- 3.1 使用 Builder ---")
	feedID := demonstrateBuilder(ctx, zm)

	// 3.2 使用原生 SQL（适合简单 SQL）
	fmt.Println("\n--- 3.2 使用原生 SQL ---")
	demonstrateRawSQL(ctx, zm, feedID)

	// 3.3 缓存 + 延迟写
	fmt.Println("\n--- 3.3 缓存 + 延迟写 ---")
	demonstrateDelayWrite(ctx, zm, feedID)

	// 3.4 查询
	fmt.Println("\n--- 3.4 查询 ---")
	demonstrateQuery(ctx, zm, feedID)

	// 3.5 删除
	fmt.Println("\n--- 3.5 删除 ---")
	demonstrateDelete(ctx, zm, feedID)

	fmt.Println("\n✓ All examples completed!")
}

// Feed 数据结构
type Feed struct {
	ID        string    `json:"id"`
	UserID    int64     `json:"user_id"`
	Content   string    `json:"content"`
	LikeCount int64     `json:"like_count"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// demonstrateBuilder 使用 Builder（适合复杂 SQL）
func demonstrateBuilder(ctx context.Context, zm zmsg.ZMsg) string {
	feedID, _ := zm.NextID(ctx, "feed")

	feed := Feed{
		ID:        feedID,
		UserID:    12345,
		Content:   "Hello, zmsg!",
		LikeCount: 0,
		Status:    "active",
		CreatedAt: time.Now(),
	}
	feedJSON, _ := json.Marshal(feed)

	// Builder 适合复杂 SQL（ON CONFLICT、批量插入等）
	task := zmsg.NewBuilder().
		Insert("feeds", map[string]interface{}{
			"id":         feed.ID,
			"user_id":    feed.UserID,
			"content":    feed.Content,
			"like_count": feed.LikeCount,
			"status":     feed.Status,
			"created_at": feed.CreatedAt,
		}).
		OnConflict("id").
		DoUpdate("content", "status").
		Build()

	zm.CacheAndStore(ctx, feedID, feedJSON, task, zmsg.WithTTL(time.Hour))
	fmt.Printf("✓ [Builder] Feed inserted: %s\n", feedID)

	return feedID
}

// demonstrateRawSQL 使用原生 SQL（适合简单 SQL）
func demonstrateRawSQL(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	// 简单更新：原生 SQL 更直观
	task := zmsg.SQL(
		"UPDATE feeds SET content = ?, status = ? WHERE id = ?",
		"Hello, zmsg! (updated via raw SQL)",
		"updated",
		feedID,
	)

	zm.SQLExec(ctx, task)
	fmt.Printf("✓ [Raw SQL] Feed updated: %s\n", feedID)

	// 也可以链式设置 TaskType
	countTask := zmsg.SQL(
		"UPDATE feeds SET like_count = like_count + ? WHERE id = ?",
		1, feedID,
	).WithType(zmsg.TaskTypeCount)

	zm.SQLExec(ctx, countTask)
	fmt.Printf("✓ [Raw SQL] Like count incremented\n")
}

// demonstrateDelayWrite 缓存 + 延迟写（高并发场景）
func demonstrateDelayWrite(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	// 模拟点赞：原生 SQL + 延迟写
	for i := 0; i < 5; i++ {
		userID := fmt.Sprintf("user_%d", i)
		likeKey := fmt.Sprintf("like:%s:%s", feedID, userID)

		// 原生 SQL + 链式设置
		task := zmsg.SQL(
			"INSERT INTO feed_likes (feed_id, user_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
			feedID, userID,
		).WithType(zmsg.TaskTypeCount).WithBatchKey("likes:" + feedID)

		zm.CacheAndDelayStore(ctx, likeKey, []byte("1"), task)
	}
	fmt.Printf("✓ [Raw SQL] 5 likes queued (async)\n")
}

// demonstrateQuery 查询
func demonstrateQuery(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	// 先检查布隆过滤器（快速判断是否存在）
	if !zm.DBHit(ctx, feedID) {
		fmt.Println("Feed not found (bloom filter)")
		return
	}

	// 查询（自动走 L1 -> L2 -> DB 管道）
	data, err := zm.Get(ctx, feedID)
	if err != nil {
		log.Printf("Failed to get feed: %v", err)
		return
	}

	var feed Feed
	if err := json.Unmarshal(data, &feed); err != nil {
		log.Printf("Failed to parse feed: %v", err)
		return
	}

	fmt.Printf("✓ Feed retrieved:\n")
	fmt.Printf("  ID: %s\n", feed.ID)
	fmt.Printf("  Content: %s\n", feed.Content)
	fmt.Printf("  UserID: %d\n", feed.UserID)
	fmt.Printf("  Status: %s\n", feed.Status)
}

// demonstrateDelete 删除
func demonstrateDelete(ctx context.Context, zm zmsg.ZMsg, feedID string) {
	// 原生 SQL 删除
	task := zmsg.SQL("DELETE FROM feeds WHERE id = ?", feedID)

	zm.DelStore(ctx, feedID, task)
	fmt.Printf("✓ [Raw SQL] Feed deleted: %s\n", feedID)
}
