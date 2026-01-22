package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yourorg/zmsg"
)

func main() {
	ctx := context.Background()

	// 配置
	cfg := zmsg.Config{
		PostgresDSN: "postgresql://user:password@localhost/zmsg_db?sslmode=disable",
		RedisAddr:   "localhost:6379",
		QueueAddr:   "localhost:6380",

		L1MaxCost:     1000000,
		DefaultTTL:    24 * time.Hour,
		BatchSize:     1000,
		BatchInterval: 5 * time.Second,

		IDPrefix:       "feed",
		MetricsEnabled: true,
		LogLevel:       "info",
	}

	// 初始化 zmsg
	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize zmsg: %v", err)
	}
	defer zm.Close()

	fmt.Println("zmsg initialized successfully")

	// 示例1: 发布 Feed
	feedContent := []byte(`{
        "user_id": 123,
        "content": "Hello, zmsg!",
        "timestamp": "2024-01-15T10:30:00Z"
    }`)

	// 自动生成 ID
	feedID, err := zm.NextID(ctx, "feed")
	if err != nil {
		log.Fatalf("Failed to generate ID: %v", err)
	}

	fmt.Printf("Generated Feed ID: %s\n", feedID)

	// 缓存并存储
	sqlTask := &zmsg.SQLTask{
		Query: `
            INSERT INTO feeds (id, user_id, content, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (id) DO UPDATE SET
                content = $3,
                updated_at = NOW()
        `,
		Params: []interface{}{
			feedID,
			123,
			string(feedContent),
		},
	}

	storedID, err := zm.CacheAndStore(ctx, feedID, feedContent, sqlTask)
	if err != nil {
		log.Fatalf("Failed to cache and store: %v", err)
	}

	fmt.Printf("Feed stored with ID: %s\n", storedID)

	// 示例2: 批量点赞（高并发场景）
	fmt.Println("\nSimulating 5000 likes...")

	for i := 0; i < 5000; i++ {
		userID := fmt.Sprintf("user_%d", i)
		likeKey := fmt.Sprintf("like:%s:%s", feedID, userID)

		sqlTask := &zmsg.SQLTask{
			Query: `
                INSERT INTO feed_likes (feed_id, user_id, liked_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (feed_id, user_id) DO UPDATE SET
                    liked_at = NOW()
            `,
			Params:   []interface{}{feedID, userID},
			TaskType: zmsg.TaskTypeCount,
			BatchKey: fmt.Sprintf("like_count:%s", feedID),
		}

		// 使用延迟存储，批处理聚合
		err := zm.CacheAndDelayStore(ctx, likeKey, []byte("1"), sqlTask)
		if err != nil {
			log.Printf("Failed to queue like: %v", err)
		}
	}

	fmt.Println("Likes queued for batch processing")

	// 示例3: 查询 Feed
	fmt.Println("\nRetrieving feed...")

	retrieved, err := zm.Get(ctx, feedID)
	if err != nil {
		log.Fatalf("Failed to retrieve feed: %v", err)
	}

	fmt.Printf("Retrieved feed: %s\n", string(retrieved))

	// 示例4: 更新 Feed
	updatedContent := []byte(`{
        "user_id": 123,
        "content": "Hello, zmsg! Updated!",
        "timestamp": "2024-01-15T10:30:00Z",
        "updated": true
    }`)

	updateTask := &zmsg.SQLTask{
		Query:  "UPDATE feeds SET content = $1 WHERE id = $2",
		Params: []interface{}{string(updatedContent), feedID},
	}

	err = zm.UpdateStore(ctx, feedID, updatedContent, updateTask)
	if err != nil {
		log.Fatalf("Failed to update feed: %v", err)
	}

	fmt.Println("Feed updated successfully")

	// 示例5: 批处理操作
	fmt.Println("\nBatch operation example...")

	batch := zm.Batch()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("batch_item_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))

		batch.CacheOnly(key, value, zmsg.WithTTL(time.Hour))
	}

	err = batch.Execute(ctx)
	if err != nil {
		log.Fatalf("Batch execution failed: %v", err)
	}

	fmt.Println("Batch operation completed")

	// 示例6: 直接执行 SQL
	fmt.Println("\nDirect SQL execution...")

	countTask := &zmsg.SQLTask{
		Query:  "SELECT COUNT(*) FROM feeds WHERE user_id = $1",
		Params: []interface{}{123},
	}

	result, err := zm.SQLExec(ctx, countTask)
	if err != nil {
		log.Fatalf("SQL execution failed: %v", err)
	}

	fmt.Printf("User 123 has %d feeds\n", result.RowsAffected)

	fmt.Println("\nAll examples completed successfully!")
}
