package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

// ============ SQL 语法糖测试（纯单元测试，不需要数据库）============

func TestSQL_Basic(t *testing.T) {
	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello")
	assert.Equal(t, "INSERT INTO feeds (id, content) VALUES ($1, $2)", task.Query)
	assert.Equal(t, []interface{}{"feed_1", "hello"}, task.Params)
}

func TestSQL_OnConflict_DoUpdate(t *testing.T) {
	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello").
		OnConflict("id").
		DoUpdate("content")

	expected := "INSERT INTO feeds (id, content) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content"
	assert.Equal(t, expected, task.Query)
}

func TestSQL_OnConflict_DoNothing(t *testing.T) {
	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello").
		OnConflict("id").
		DoNothing()

	expected := "INSERT INTO feeds (id, content) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
	assert.Equal(t, expected, task.Query)
}

func TestSQL_OnConflict_MultipleColumns(t *testing.T) {
	task := zmsg.SQL("INSERT INTO likes (user_id, feed_id) VALUES (?, ?)", "user_1", "feed_1").
		OnConflict("user_id", "feed_id").
		DoNothing()

	expected := "INSERT INTO likes (user_id, feed_id) VALUES ($1, $2) ON CONFLICT (user_id, feed_id) DO NOTHING"
	assert.Equal(t, expected, task.Query)
}

func TestSQL_Returning(t *testing.T) {
	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello").
		OnConflict("id").
		DoNothing().
		Returning("id", "created_at")

	expected := "INSERT INTO feeds (id, content) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING id, created_at"
	assert.Equal(t, expected, task.Query)
}

// ============ Counter 语法糖测试 ============

func TestCounter_Inc(t *testing.T) {
	task := zmsg.Counter("feed_meta", "like_count").
		Inc(1).
		Where("id = ?", "feed_1").
		BatchKey("meta:feed_1").
		Build()

	assert.Equal(t, "UPDATE feed_meta SET like_count = like_count + $1 WHERE id = $2", task.Query)
	assert.Equal(t, []interface{}{1, "feed_1"}, task.Params)
	assert.Equal(t, zmsg.TaskTypeCounter, task.TaskType)
	assert.Equal(t, "meta:feed_1", task.BatchKey)
}

func TestCounter_Dec(t *testing.T) {
	task := zmsg.Counter("feed_meta", "like_count").
		Dec(5).
		Where("id = ?", "feed_1").
		Build()

	assert.Equal(t, "UPDATE feed_meta SET like_count = like_count - $1 WHERE id = $2", task.Query)
	assert.Equal(t, []interface{}{5, "feed_1"}, task.Params)
}

func TestCounter_Mul(t *testing.T) {
	task := zmsg.Counter("feed_meta", "score").
		Mul(2).
		Where("id = ?", "feed_1").
		Build()

	assert.Equal(t, "UPDATE feed_meta SET score = score * $1 WHERE id = $2", task.Query)
}

func TestCounter_Clean(t *testing.T) {
	task := zmsg.Counter("feed_meta", "like_count").
		Clean().
		Where("id = ?", "feed_1").
		Build()

	assert.Equal(t, "UPDATE feed_meta SET like_count = 0 WHERE id = $1", task.Query)
}

func TestCounter_TableColumnStyle(t *testing.T) {
	task := zmsg.Table("feed_reply_meta").Column("like_count").Counter().
		Inc(1).
		Where("id = ?", "feed_1").
		BatchKey("meta:feed_1").
		Build()

	assert.Equal(t, "UPDATE feed_reply_meta SET like_count = like_count + $1 WHERE id = $2", task.Query)
	assert.Equal(t, zmsg.TaskTypeCounter, task.TaskType)
}

// ============ Slice 语法糖测试 ============

func TestSlice_Add(t *testing.T) {
	task := zmsg.Slice("feed_meta", "tags").
		Add("golang").
		Where("id = ?", "feed_1").
		BatchKey("meta:feed_1").
		Build()

	assert.Contains(t, task.Query, "UPDATE feed_meta SET tags")
	assert.Equal(t, zmsg.TaskTypeAppend, task.TaskType)
}

func TestSlice_Del(t *testing.T) {
	task := zmsg.Slice("feed_meta", "tags").
		Del("golang").
		Where("id = ?", "feed_1").
		Build()

	assert.Contains(t, task.Query, "UPDATE feed_meta SET tags")
}

func TestSlice_Clean(t *testing.T) {
	task := zmsg.Slice("feed_meta", "tags").
		Clean().
		Where("id = ?", "feed_1").
		Build()

	assert.Contains(t, task.Query, "UPDATE feed_meta SET tags = '[]'::jsonb")
}

// ============ Map 语法糖测试 ============

func TestMap_Set(t *testing.T) {
	task := zmsg.Map("feed_meta", "extra").
		Set("view_count", 100).
		Where("id = ?", "feed_1").
		BatchKey("meta:feed_1").
		Build()

	assert.Contains(t, task.Query, "UPDATE feed_meta SET extra")
	assert.Equal(t, zmsg.TaskTypePut, task.TaskType)
}

func TestMap_Del(t *testing.T) {
	task := zmsg.Map("feed_meta", "extra").
		Del("view_count").
		Where("id = ?", "feed_1").
		Build()

	assert.Contains(t, task.Query, "UPDATE feed_meta SET extra = extra - $1")
}

// ============ 集成测试（需要数据库）============

func TestIntegration_CacheAndStore(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()
	ctx := context.Background()

	feedID := fmt.Sprintf("feed_%d", time.Now().UnixNano())
	feedData := map[string]interface{}{
		"id":      feedID,
		"content": "Hello, zmsg!",
	}
	feedJSON, _ := json.Marshal(feedData)

	// 写入
	task := zmsg.SQL("INSERT INTO test_feeds (id, content) VALUES (?, ?)", feedID, "Hello, zmsg!").
		OnConflict("id").
		DoUpdate("content")

	_, err := zm.CacheAndStore(ctx, feedID, feedJSON, task)
	require.NoError(t, err)

	// 读取
	result, err := zm.Get(ctx, feedID)
	require.NoError(t, err)
	assert.Equal(t, feedJSON, result)

	// 清理
	zm.Del(ctx, feedID)
}

func TestIntegration_CacheOnly(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()
	ctx := context.Background()

	key := fmt.Sprintf("cache_only_%d", time.Now().UnixNano())
	value := []byte("test_value")

	// 仅缓存
	err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
	require.NoError(t, err)

	// 读取
	result, err := zm.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, result)

	// 清理
	zm.Del(ctx, key)
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
	ctx := context.Background()

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
			counterTask := zmsg.Table("feed_meta").Column("like_count").Counter().
				Inc(1).
				Where("id = ?", feedID).
				BatchKey("counter:" + feedID).
				Build()

			err := zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("counter:%s:%d", feedID, idx), nil, counterTask)
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
	ctx := context.Background()

	feedID := fmt.Sprintf("feed_del_%d", time.Now().UnixNano())

	// 先创建
	task := zmsg.SQL("INSERT INTO test_feeds (id, content) VALUES (?, ?)", feedID, "to_delete").
		OnConflict("id").
		DoNothing()
	_, err := zm.CacheAndStore(ctx, feedID, []byte("{}"), task)
	require.NoError(t, err)

	// 使用 UPDATE 标记删除（软删除）代替 DELETE
	softDelTask := zmsg.SQL("UPDATE test_feeds SET status = ? WHERE id = ?", "deleted", feedID)
	err = zm.DelStore(ctx, feedID, softDelTask)
	require.NoError(t, err)

	// 验证缓存已删除
	_, err = zm.Get(ctx, feedID)
	assert.Error(t, err)
}

func TestIntegration_Update(t *testing.T) {
	zm := testutil.NewZmsg(t)
	defer zm.Close()
	ctx := context.Background()

	key := fmt.Sprintf("update_test_%d", time.Now().UnixNano())

	// 先写入
	err := zm.CacheOnly(ctx, key, []byte("original"), zmsg.WithTTL(time.Minute))
	require.NoError(t, err)

	// 更新
	err = zm.Update(ctx, key, []byte("updated"))
	require.NoError(t, err)

	// 验证
	result, err := zm.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("updated"), result)

	// 清理
	zm.Del(ctx, key)
}

func TestIntegration_DBHit(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()
	ctx := context.Background()

	feedID := fmt.Sprintf("bloom_test_%d", time.Now().UnixNano())

	// 写入数据
	task := zmsg.SQL("INSERT INTO test_feeds (id, content) VALUES (?, ?)", feedID, "bloom test").
		OnConflict("id").
		DoNothing()
	_, err := zm.CacheAndStore(ctx, feedID, []byte("{}"), task)
	require.NoError(t, err)

	// 检查 bloom filter
	hit := zm.DBHit(ctx, feedID)
	assert.True(t, hit, "bloom filter should return true for existing key")

	// 不存在的 key
	nonExistKey := fmt.Sprintf("non_exist_%d", time.Now().UnixNano())
	hit = zm.DBHit(ctx, nonExistKey)
	// 布隆过滤器可能有误判，但通常不存在的 key 应该返回 false
	t.Logf("DBHit for non-exist key: %v", hit)

	// 清理
	zm.Del(ctx, feedID)
}

func TestIntegration_SQLExec(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()
	ctx := context.Background()

	feedID := fmt.Sprintf("exec_test_%d", time.Now().UnixNano())

	// INSERT
	insertTask := zmsg.SQL("INSERT INTO test_feeds (id, content, like_count) VALUES (?, ?, ?)", feedID, "exec test", 10).
		OnConflict("id").
		DoNothing()
	result, err := zm.SQLExec(ctx, insertTask)
	require.NoError(t, err)
	t.Logf("INSERT result: RowsAffected=%d", result.RowsAffected)

	// UPDATE (增加计数)
	updateTask := zmsg.SQL("UPDATE test_feeds SET like_count = like_count + ? WHERE id = ?", 5, feedID)
	result, err = zm.SQLExec(ctx, updateTask)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	// UPDATE (软删除)
	softDeleteTask := zmsg.SQL("UPDATE test_feeds SET status = ? WHERE id = ?", "deleted", feedID)
	result, err = zm.SQLExec(ctx, softDeleteTask)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)
}

func TestIntegration_ConcurrentWrites(t *testing.T) {
	zm := testutil.NewZmsgWithSchema(t)
	defer zm.Close()
	ctx := context.Background()

	// 并发写入多个 feed
	var wg sync.WaitGroup
	concurrency := 100

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			feedID := fmt.Sprintf("concurrent_feed_%d_%d", time.Now().UnixNano(), idx)
			value := []byte(fmt.Sprintf(`{"id":"%s","idx":%d}`, feedID, idx))

			task := zmsg.SQL("INSERT INTO test_feeds (id, content) VALUES (?, ?)", feedID, fmt.Sprintf("content_%d", idx)).
				OnConflict("id").
				DoNothing()

			_, err := zm.CacheAndStore(ctx, feedID, value, task)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	t.Logf("Concurrent writes completed: %d operations", concurrency)
}
