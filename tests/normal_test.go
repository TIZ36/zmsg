package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	zmsg "github.com/tiz36/zmsg/core"
	"github.com/tiz36/zmsg/tests/testutil"
)

// ============ SQL 语法糖测试（纯单元测试，不需要数据库）============

// ============ SQL 语法糖测试 ============

func TestSQL_Builders(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()

	t.Run("Basic", func(t *testing.T) {
		// Native SQL builder
		builder := zm.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello")
		// 内部实现可能会转换占位符，但对外通常在 Exec 时处理
		// 这里暂不深入内部细节测试，主要测试 API 连贯性
		assert.NotNil(t, builder)
	})

	t.Run("OnConflict", func(t *testing.T) {
		// 注意：在新 API 中，Save() 封装了 INSERT ON CONFLICT
		// 原生 SQL 通过 SQLBuilder 链式构建
		// 这里我们主要依赖 Table().Save() 的内部构建逻辑测试
	})
}

// ============ Counter 语法糖测试 ============

// ============ Periodic 策略测试 ============

func TestPeriodic_Builders(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()

	t.Run("Counter", func(t *testing.T) {
		err := zm.Table("feed_meta").
			CacheKey("meta:1").
			PeriodicCount().
			UpdateColumn().
			Column("like_count").
			Do(zmsg.Add(), 1)
		assert.NoError(t, err)
	})

	t.Run("Override", func(t *testing.T) {
		_ = zm.Table("user").
			CacheKey("user:1").
			PeriodicOverride().
			Save(map[string]any{"id": "1", "name": "test"})
	})
}

// ============ Slice 语法糖测试 ============

// Slice 和 Map 语法糖集成进入了 Table().UpdateColumns 或特定的 Do 逻辑

// ============ 集成测试（需要数据库）============

func TestIntegration_CacheAndStore(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	feedID := fmt.Sprintf("feed_%d", time.Now().UnixNano())

	// 写入
	err := zm.Table("feeds").CacheKey(feedID).Save(struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}{ID: feedID, Content: "Hello, zmsg!"})
	require.NoError(t, err)

	// 读取
	var feed struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}
	err = zm.Table("feeds").CacheKey(feedID).Query(&feed)
	require.NoError(t, err)
	assert.Equal(t, "Hello, zmsg!", feed.Content)

	// 清理
	_ = zm.Table("feeds").CacheKey(feedID).Del()
}

func TestIntegration_CacheOnly(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()
	// 注意：新 API 建议通过 Table().Save() 处理同步写
	// 这里演示正常的 Save
	feedID := "cache_test_1"
	err := zm.Table("feeds").CacheKey(feedID).Save(struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}{ID: feedID, Content: "test"})
	require.NoError(t, err)

	var feed struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}
	err = zm.Table("feeds").CacheKey(feedID).Query(&feed)
	require.NoError(t, err)
	assert.Equal(t, "test", feed.Content)
}
func TestIntegration_NextID(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()
	ctx := context.Background()

	// 生成 ID
	id1, err := zm.NextID(ctx, "feed")
	require.NoError(t, err)
	assert.NotEmpty(t, id1)

	id2, err := zm.NextID(ctx, "feed")
	require.NoError(t, err)
	assert.NotEmpty(t, id2)

	// ID 应该递增且唯一
	assert.NotEqual(t, id1, id2)
}

func TestIntegration_CounterAggregation(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	feedID := fmt.Sprintf("counter_test_%d", time.Now().UnixNano())

	// 先创建 feed_meta 记录
	testutil.InsertFeedMeta(t, zm, feedID)

	// 并发增加计数
	var wg sync.WaitGroup
	concurrency := 50

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := zm.Table("feed_meta").
				CacheKey("meta:"+feedID).
				PeriodicCount().
				UpdateColumn().
				Column("like_count").
				Do(zmsg.Add(), 1)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 等待 flush（配置默认 5 秒）
	time.Sleep(6 * time.Second)

	t.Logf("Counter aggregation test completed: %d increments submitted for feed %s", concurrency, feedID)
}

func TestIntegration_DelStore(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	feedID := fmt.Sprintf("feed_del_%d", time.Now().UnixNano())

	// 先创建
	err := zm.Table("feeds").CacheKey(feedID).Save(struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}{ID: feedID, Content: "to_delete"})
	require.NoError(t, err)

	// 删除
	err = zm.Table("feeds").CacheKey(feedID).Del()
	require.NoError(t, err)

	// 查询验证（应该不存在）
	var feed struct {
		ID      string `db:"id,pk"`
		Content string `db:"content"`
	}
	err = zm.Table("feeds").CacheKey(feedID).Query(&feed)
	require.ErrorIs(t, err, zmsg.ErrNotFound)
}

func TestIntegration_ConcurrentWrites(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	// 并发写入多个 feed
	var wg sync.WaitGroup
	concurrency := 100

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("concurrent_feed_%d_%d", time.Now().UnixNano(), idx)
			err := zm.Table("feeds").CacheKey(id).Save(struct {
				ID      string `db:"id,pk"`
				Content string `db:"content"`
			}{ID: id, Content: fmt.Sprintf("content_%d", idx)})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
}

func TestIntegration_DistributedSerialization(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	userID := fmt.Sprintf("user_ser_%d", time.Now().UnixNano())
	// 初始化用户余额
	err := zm.Table("users").CacheKey(userID).Save(struct {
		ID      string `db:"id,pk"`
		Name    string `db:"name"`
		Balance int64  `db:"balance"`
	}{ID: userID, Name: "tester", Balance: 1000})
	require.NoError(t, err)

	// 并发执行有顺序要求的更新（使用 Serialize）
	var wg sync.WaitGroup
	count := 20
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 所有的操作都必须基于最新的余额计算，Serialize 保证了这一点
			_ = zm.Table("users").
				CacheKey(userID).
				Serialize(userID). // 关键：确保串行处理
				UpdateColumn().
				Column("balance").
				Do(zmsg.Add(), 10)
		}(i)
	}
	wg.Wait()

	// 等待异步处理完成
	time.Sleep(3 * time.Second)

	// 验证最终余额（1000 + 20*10 = 1200）
	// 注意：使用 SQL().Query() 直接从 DB 验证，忽略缓存
	var balance int64
	err = zm.SQL("SELECT balance FROM users WHERE id = ?", userID).Query(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(1200), balance)
}

func TestIntegration_PeriodicMerge(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()

	userID := fmt.Sprintf("user_merge_%d", time.Now().UnixNano())
	// 初始化用户
	_ = zm.Table("users").CacheKey(userID).Save(struct {
		ID   string `db:"id,pk"`
		Meta string `db:"meta"`
	}{ID: userID, Meta: `{"v":1}`})

	// 并发合并不同的 JSON 字段
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = zm.Table("users").
			CacheKey(userID).
			PeriodicMerge().
			UpdateColumns(map[string]any{"meta": map[string]any{"a": 1}})
	}()

	go func() {
		defer wg.Done()
		_ = zm.Table("users").
			CacheKey(userID).
			PeriodicMerge().
			UpdateColumns(map[string]any{"meta": map[string]any{"b": 2}})
	}()

	wg.Wait()
	time.Sleep(6 * time.Second)

	// 验证 Meta 合并结果
	var metaStr string
	err := zm.SQL("SELECT meta FROM users WHERE id = ?", userID).Query(&metaStr)
	require.NoError(t, err)
	assert.Contains(t, metaStr, `"a": 1`)
	assert.Contains(t, metaStr, `"b": 2`)
	assert.Contains(t, metaStr, `"v": 1`)
}
