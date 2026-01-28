package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	zmsg "github.com/tiz36/zmsg/core"
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
	ReplyCount int64    `db:"reply_count"`
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
		reply_count BIGINT DEFAULT 0,
		tags JSONB DEFAULT '[]',
		created_at TIMESTAMP DEFAULT NOW()
	);
	ALTER TABLE threads ADD COLUMN IF NOT EXISTS reply_count BIGINT DEFAULT 0;
	CREATE TABLE IF NOT EXISTS feeds (
		id TEXT PRIMARY KEY,
		user_id BIGINT,
		content TEXT,
		like_count BIGINT DEFAULT 0,
		status TEXT,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS likes (
		user_id TEXT,
		thread_id TEXT,
		created_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY (user_id, thread_id)
	);
	CREATE TABLE IF NOT EXISTS replies (
		id TEXT PRIMARY KEY,
		feed_id TEXT,
		user_id TEXT,
		parent_id TEXT,
		content TEXT,
		seq BIGINT DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
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
	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	runPrefix := "run:" + runID + ":"

	// 场景 A: 创建用户 (同步写)
	fmt.Println("\n--- [A] Creating Users (Sync) ---")
	users := make([]*User, 5)
	for i := 0; i < 5; i++ {
		uid, _ := zm.NextID(ctx, "user")
		user := &User{
			ID:        uid,
			Name:      fmt.Sprintf("User_%d_%s", i, uid),
			Balance:   1000,
			Meta:      "{}",
			CreatedAt: time.Now(),
		}
		if err := zm.Table("users").CacheKey(uid).Save(user); err != nil {
			log.Printf("Failed to create user: %v", err)
		}
		users[i] = user
		fmt.Printf("Created User: %s (Balance: %d)\n", uid, user.Balance)
	}

	// 场景 A2: 基础查询 (Query(dest))
	fmt.Println("\n--- [A2] Basic Query (Cache -> DB) ---")
	var userByID User
	if err := zm.Table("users").CacheKey(users[0].ID).Query(&userByID); err != nil {
		log.Printf("Query by ID failed: %v", err)
	} else {
		fmt.Printf("Fetched User by ID: %s (%s)\n", userByID.ID, userByID.Name)
		assertf(userByID.ID == users[0].ID, "user id mismatch: %s != %s", userByID.ID, users[0].ID)
	}
	// CacheKey 非主键时，建议加 Where
	var userByName User
	if err := zm.Table("users").
		CacheKey("user_name:"+users[0].Name).
		Where("name = ?", users[0].Name).
		Query(&userByName); err != nil {
		log.Printf("Query by name failed: %v", err)
	} else {
		fmt.Printf("Fetched User by name: %s (%s)\n", userByName.ID, userByName.Name)
		assertf(userByName.ID == users[0].ID, "user name lookup mismatch: %s != %s", userByName.ID, users[0].ID)
	}

	// 场景 B: 发布帖子 (异步/周期写) -> 提高吞吐
	fmt.Println("\n--- [B] Creating Threads (Periodic/Override) ---")
	threadID, _ := zm.NextID(ctx, "thread")
	thread := &Thread{
		ID:        threadID,
		AuthorID:  users[0].ID,
		Content:   runPrefix + "Hello zmsg world! High performance storage.",
		LikeCount: 0,
		ReplyCount: 0,
		Tags:      "[]",
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
				PeriodicCount().    // 启用计数器聚合策略
				UpdateColumn().Column("like_count").
				Do(zmsg.Add(), 1)
			if err != nil {
				log.Printf("Like failed: %v", err)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Processed %d likes in %v (Requests merged in memory)\n", likeCount, time.Since(start))

	// 等待聚合刷新到 DB
	if err := waitUntil("like_count", 90*time.Second, 2*time.Second, func() (bool, error) {
		var threadAfterLikes Thread
		if err := zm.SQL("SELECT id, author_id, content, like_count, reply_count, tags, created_at FROM threads WHERE id = ?", threadID).
			Query(&threadAfterLikes); err != nil {
			if errors.Is(err, zmsg.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return threadAfterLikes.LikeCount == int64(likeCount), nil
	}); err != nil {
		log.Printf("like_count not ready: %v", err)
	} else {
		fmt.Printf("like_count validated (%d)\n", likeCount)
	}

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

	// 等待合并刷新到 DB
	if err := waitUntil("user_meta", 30*time.Second, 2*time.Second, func() (bool, error) {
		var userMeta struct {
			Meta string `db:"meta"`
		}
		if err := zm.SQL("SELECT meta FROM users WHERE id = ?", targetUser).Query(&userMeta); err != nil {
			return false, err
		}
		return strings.Contains(userMeta.Meta, `"theme"`) && strings.Contains(userMeta.Meta, `"lang"`), nil
	}); err != nil {
		log.Printf("user meta not ready: %v", err)
	} else {
		fmt.Println("user meta validated")
	}

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

	// 校验余额
	expectedBalance := int64(1000 - balanceOps*10)
	if err := waitUntil("balance", 10*time.Second, 1*time.Second, func() (bool, error) {
		var balanceRow struct {
			Balance int64 `db:"balance"`
		}
		if err := zm.SQL("SELECT balance FROM users WHERE id = ?", targetUser).Query(&balanceRow); err != nil {
			return false, err
		}
		return balanceRow.Balance == expectedBalance, nil
	}); err != nil {
		log.Printf("balance not ready: %v", err)
	} else {
		fmt.Printf("balance validated (%d)\n", expectedBalance)
	}

	// 场景 F: 列表查询 (Query Slice Scan)
	fmt.Println("\n--- [F] List Query (Smart Slice Scan) ---")
	time.Sleep(1 * time.Second) // 等待部分异步写入完成

	var threadList []*Thread
	// 查询该作者的所有帖子
	query := "SELECT id, author_id, content, like_count, reply_count, created_at FROM threads WHERE author_id = ? ORDER BY created_at DESC"

	// zmsg 自动识别 []*Thread 并执行 Query + Scan
	err := zm.SQL(query, users[0].ID).Query(&threadList)
	if err != nil {
		log.Printf("Query list failed: %v", err)
	} else {
		fmt.Printf("Found %d threads for user %s\n", len(threadList), users[0].Name)
		assertf(len(threadList) > 0, "thread list is empty")
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
	err = zm.SQL(joinQuery, minLikes, authorFilter, limit).Query(&joinResults)
	if err != nil {
		log.Printf("JOIN query failed: %v", err)
	} else {
		fmt.Printf("Found %d threads (likes >= %d) by author %s:\n",
			len(joinResults), minLikes, users[0].Name)
		assertf(len(joinResults) > 0, "join results empty")
		for _, r := range joinResults {
			fmt.Printf(" - Thread[%s] by %s (Balance: %d): %s (Likes: %d)\n",
				r.ThreadID[:8], r.AuthorName, r.AuthorBalance, r.Content, r.LikeCount)
		}
	}

	// 场景 H: 并发写入校验 (Save)
	fmt.Println("\n--- [H] Concurrent Saves (Assert Count) ---")
	feedCount := 20
	var wgs sync.WaitGroup
	var feedIDs []string
	var feedMu sync.Mutex
	for i := 0; i < feedCount; i++ {
		wgs.Add(1)
		go func(idx int) {
			defer wgs.Done()
			id, _ := zm.NextID(ctx, "feed")
			feed := Feed{
				ID:      id,
				UserID:  int64(100 + idx),
				Content: fmt.Sprintf("%sfeed_%d", runPrefix, idx),
				Status:  "active",
			}
			if err := zm.Table("feeds").CacheKey(id).Save(feed); err != nil {
				log.Printf("Failed to save feed: %v", err)
				return
			}
			feedMu.Lock()
			feedIDs = append(feedIDs, id)
			feedMu.Unlock()
		}(i)
	}
	wgs.Wait()

	var feedRows []Feed
	if err := zm.SQL("SELECT id, user_id, content, status, created_at FROM feeds WHERE content LIKE ? ORDER BY created_at DESC", runPrefix+"%").
		Query(&feedRows); err != nil {
		log.Printf("Query feeds failed: %v", err)
	} else {
		assertf(len(feedRows) == feedCount, "feed count mismatch: %d != %d", len(feedRows), feedCount)
		fmt.Printf("feed count validated (%d)\n", feedCount)
	}

	// 场景 I: 删除校验（缓存 + DB）
	fmt.Println("\n--- [I] Delete Validation ---")
	deleteCount := 5
	if deleteCount > len(feedIDs) {
		deleteCount = len(feedIDs)
	}
	for i := 0; i < deleteCount; i++ {
		if err := zm.Table("feeds").CacheKey(feedIDs[i]).Del(); err != nil {
			log.Printf("Failed to delete feed: %v", err)
		}
		var deleted Feed
		err := zm.Table("feeds").CacheKey(feedIDs[i]).Query(&deleted)
		assertf(errors.Is(err, zmsg.ErrNotFound), "deleted feed still exists: %v", err)
	}

	var remaining []Feed
	if err := zm.SQL("SELECT id, user_id, content, status, created_at FROM feeds WHERE content LIKE ?", runPrefix+"%").
		Query(&remaining); err != nil {
		log.Printf("Query remaining feeds failed: %v", err)
	} else {
		expectedRemaining := feedCount - deleteCount
		assertf(len(remaining) == expectedRemaining, "remaining feed count mismatch: %d != %d", len(remaining), expectedRemaining)
		fmt.Printf("delete validated (%d removed)\n", deleteCount)
	}

	// 场景 J: 多列聚合计数（PeriodicCount）
	fmt.Println("\n--- [J] Multi Counter Aggregation ---")
	counterThreadID, _ := zm.NextID(ctx, "thread")
	counterThread := &Thread{
		ID:         counterThreadID,
		AuthorID:   users[0].ID,
		Content:    runPrefix + "counter thread",
		LikeCount:  0,
		ReplyCount: 0,
		Tags:       "[]",
		CreatedAt:  time.Now(),
	}
	if err := zm.Table("threads").CacheKey(counterThreadID).Save(counterThread); err != nil {
		log.Printf("Failed to create counter thread: %v", err)
	}
	likeN := 200
	replyN := 120
	var wgCounter sync.WaitGroup
	for i := 0; i < likeN; i++ {
		wgCounter.Add(1)
		go func() {
			defer wgCounter.Done()
			_ = zm.Table("threads").CacheKey(counterThreadID).
				PeriodicCount().UpdateColumn().Column("like_count").
				Do(zmsg.Add(), 1)
		}()
	}
	for i := 0; i < replyN; i++ {
		wgCounter.Add(1)
		go func() {
			defer wgCounter.Done()
			_ = zm.Table("threads").CacheKey(counterThreadID).
				PeriodicCount().UpdateColumn().Column("reply_count").
				Do(zmsg.Add(), 1)
		}()
	}
	wgCounter.Wait()
	if err := waitUntil("multi_counter", 60*time.Second, 2*time.Second, func() (bool, error) {
		var row Thread
		if err := zm.SQL("SELECT id, like_count, reply_count FROM threads WHERE id = ?", counterThreadID).Query(&row); err != nil {
			return false, err
		}
		return row.LikeCount == int64(likeN) && row.ReplyCount == int64(replyN), nil
	}); err != nil {
		log.Printf("multi counter not ready: %v", err)
	} else {
		fmt.Printf("multi counter validated (likes=%d, replies=%d)\n", likeN, replyN)
	}

	// 场景 K: Serialize + PeriodicOverride 叠加
	fmt.Println("\n--- [K] Serialize + PeriodicOverride ---")
	overrideThreadID, _ := zm.NextID(ctx, "thread")
	overrideCount := 10
	expectedContents := make(map[string]struct{}, overrideCount)
	var wgOverride sync.WaitGroup
	for i := 0; i < overrideCount; i++ {
		wgOverride.Add(1)
		content := fmt.Sprintf("%soverride_%d", runPrefix, i)
		expectedContents[content] = struct{}{}
		go func(c string) {
			defer wgOverride.Done()
			_ = zm.Table("threads").CacheKey(overrideThreadID).
				Serialize(overrideThreadID).
				PeriodicOverride().
				Save(&Thread{
					ID:         overrideThreadID,
					AuthorID:   users[0].ID,
					Content:    c,
					LikeCount:  0,
					ReplyCount: 0,
					Tags:       "[]",
					CreatedAt:  time.Now(),
				})
		}(content)
	}
	wgOverride.Wait()
	if err := waitUntil("override_content", 60*time.Second, 2*time.Second, func() (bool, error) {
		var row Thread
		if err := zm.SQL("SELECT id, content FROM threads WHERE id = ?", overrideThreadID).Query(&row); err != nil {
			if errors.Is(err, zmsg.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		_, ok := expectedContents[row.Content]
		return ok, nil
	}); err != nil {
		log.Printf("override content not ready: %v", err)
	} else {
		fmt.Println("override content validated")
	}

	// 场景 L: 并发 Save + Del 交错
	fmt.Println("\n--- [L] Concurrent Save + Del ---")
	raceID, _ := zm.NextID(ctx, "feed")
	_ = zm.Table("feeds").CacheKey(raceID).Save(&Feed{
		ID:      raceID,
		UserID:  999,
		Content: runPrefix + "race_init",
		Status:  "active",
	})
	raceOps := 50
	var wgRace sync.WaitGroup
	for i := 0; i < raceOps; i++ {
		wgRace.Add(1)
		go func(idx int) {
			defer wgRace.Done()
			if idx%2 == 0 {
				_ = zm.Table("feeds").CacheKey(raceID).Save(&Feed{
					ID:      raceID,
					UserID:  999,
					Content: fmt.Sprintf("%srace_save_%d", runPrefix, idx),
					Status:  "active",
				})
			} else {
				_ = zm.Table("feeds").CacheKey(raceID).Del()
			}
		}(i)
	}
	wgRace.Wait()
	finalContent := runPrefix + "race_final"
	_ = zm.Table("feeds").CacheKey(raceID).Save(&Feed{
		ID:      raceID,
		UserID:  999,
		Content: finalContent,
		Status:  "active",
	})
	var finalFeed Feed
	if err := zm.Table("feeds").CacheKey(raceID).Query(&finalFeed); err != nil {
		log.Printf("race final query failed: %v", err)
	} else {
		assertf(finalFeed.Content == finalContent, "race final content mismatch: %s != %s", finalFeed.Content, finalContent)
		fmt.Println("save+del race validated")
	}

	// 场景 M: SQL Query + CacheKey + 标量扫描
	fmt.Println("\n--- [M] SQL Cache Query (Scalar Scan) ---")
	var threadTotal int64
	countKey := "thread_count:" + users[0].ID
	if err := zm.SQL("SELECT COUNT(*) FROM threads WHERE author_id = ?", users[0].ID).
		CacheKey(countKey).
		Query(&threadTotal); err != nil {
		log.Printf("thread count query failed: %v", err)
	} else {
		assertf(threadTotal > 0, "thread count should be > 0, got %d", threadTotal)
		fmt.Printf("thread count validated (%d)\n", threadTotal)
	}

	// 场景 N: NotFound 行为校验（Query -> Save -> Del）
	fmt.Println("\n--- [N] NotFound Validation ---")
	missingID := runPrefix + "missing_feed"
	missingKey := "feed:" + missingID
	var missing Feed
	err = zm.Table("feeds").
		CacheKey(missingKey).
		Where("id = ?", missingID).
		Query(&missing)
	assertf(errors.Is(err, zmsg.ErrNotFound), "expected not found, got: %v", err)
	_ = zm.Table("feeds").CacheKey(missingKey).Save(&Feed{
		ID:      missingID,
		UserID:  888,
		Content: runPrefix + "missing_feed_content",
		Status:  "active",
	})
	var found Feed
	if err := zm.Table("feeds").
		CacheKey(missingKey).
		Where("id = ?", missingID).
		Query(&found); err != nil {
		log.Printf("missing feed query after save failed: %v", err)
	} else {
		assertf(found.ID == missingID, "missing feed id mismatch: %s != %s", found.ID, missingID)
		fmt.Println("missing feed saved and found")
	}
	_ = zm.Table("feeds").
		CacheKey(missingKey).
		Where("id = ?", missingID).
		Del()
	if err := waitUntil("missing_feed_delete", 10*time.Second, 1*time.Second, func() (bool, error) {
		err := zm.Table("feeds").
			CacheKey(missingKey).
			Where("id = ?", missingID).
			Query(&missing)
		if errors.Is(err, zmsg.ErrNotFound) {
			return true, nil
		}
		return false, err
	}); err != nil {
		log.Printf("missing feed delete not ready: %v", err)
	} else {
		fmt.Println("missing feed delete validated")
	}

	// 场景 O: Serialize 顺序校验（按提交顺序落库）
	fmt.Println("\n--- [O] Serialize Order Validation ---")
	serializeFeedID, _ := zm.NextID(ctx, "feed")
	replyCount := 30
	var submitSeq int64
	var wgSerialize sync.WaitGroup
	for i := 0; i < replyCount; i++ {
		wgSerialize.Add(1)
		go func() {
			defer wgSerialize.Done()
			seq := atomic.AddInt64(&submitSeq, 1)
			replyID, _ := zm.NextID(ctx, "reply")
			reply := Reply{
				ID:      replyID,
				FeedID:  serializeFeedID,
				UserID:  users[0].ID,
				Content: fmt.Sprintf("%sreply_%d", runPrefix, seq),
				Seq:     seq,
			}
			if err := zm.Table("replies").
				CacheKey("reply:" + replyID).
				Serialize("feed_replies:" + serializeFeedID).
				Save(reply); err != nil {
				log.Printf("serialize reply save failed: %v", err)
			}
		}()
	}
	wgSerialize.Wait()

	var replyRows []Reply
	if err := waitUntil("serialize_reply_count", 30*time.Second, 1*time.Second, func() (bool, error) {
		replyRows = replyRows[:0]
		if err := zm.SQL("SELECT id, feed_id, user_id, parent_id, content, seq, created_at, updated_at FROM replies WHERE feed_id = ? ORDER BY seq ASC, id ASC", serializeFeedID).
			Query(&replyRows); err != nil {
			if errors.Is(err, zmsg.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return len(replyRows) == replyCount, nil
	}); err != nil {
		log.Printf("serialize reply count not ready: %v", err)
	} else {
		for i, r := range replyRows {
			expectedSeq := int64(i + 1)
			expectedContent := fmt.Sprintf("%sreply_%d", runPrefix, expectedSeq)
			assertf(r.Seq == expectedSeq, "serialize order violated at %d: expected=%d got=%d", i, expectedSeq, r.Seq)
			assertf(r.Content == expectedContent, "serialize content mismatch at %d: %s != %s", i, r.Content, expectedContent)
		}
		fmt.Println("serialize order validated")
	}
}

func assertf(cond bool, format string, args ...any) {
	if !cond {
		log.Fatalf("ASSERT FAILED: "+format, args...)
	}
}

func waitUntil(name string, timeout, interval time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err == nil && ok {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("%s timeout: %w", name, err)
			}
			return fmt.Errorf("%s timeout", name)
		}
		time.Sleep(interval)
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
	Seq       int64     `json:"seq" db:"seq"`
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
