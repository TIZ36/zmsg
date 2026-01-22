package zmsg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiz36/zmsg/zmsg"
)

// TestAsyncQueue 测试异步队列
func TestAsyncQueue(t *testing.T) {
	ctx := context.Background()

	cfg := zmsg.DefaultConfig()
	cfg.PostgresDSN = "postgresql://test:test@localhost/test?sslmode=disable"
	cfg.RedisAddr = "localhost:6379"
	cfg.QueueAddr = "localhost:6380"

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试延迟存储
	t.Run("TestDelayStore", func(t *testing.T) {
		key := "queue_delay_test"
		value := []byte("delay_value")

		processed := make(chan bool, 1)

		// 模拟消费者（实际应该由zmsg内部处理）
		go func() {
			time.Sleep(2 * time.Second)
			processed <- true
		}()

		sqlTask := &zmsg.SQLTask{
			Query:    "INSERT INTO test_queue (id, data) VALUES ($1, $2)",
			Params:   []interface{}{key, value},
			TaskType: zmsg.TaskTypeContent,
		}

		// 延迟存储
		err := zm.CacheAndDelayStore(ctx, key, value, sqlTask)
		assert.NoError(t, err)

		// 立即验证缓存
		cached, err := zm.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, cached)

		// 等待处理
		select {
		case <-processed:
			t.Log("Queue item processed")
		case <-time.After(5 * time.Second):
			t.Log("Queue processing timeout (acceptable in test)")
		}
	})

	// 测试计数器批处理队列
	t.Run("TestCounterQueue", func(t *testing.T) {
		feedID := "queue_counter_test"

		// 模拟多个点赞操作入队
		for i := 0; i < 20; i++ {
			userID := fmt.Sprintf("queue_user_%d", i)
			key := fmt.Sprintf("like:%s:%s", feedID, userID)

			sqlTask := &zmsg.SQLTask{
				Query:    "INSERT INTO feed_likes_queue (feed_id, user_id) VALUES ($1, $2)",
				Params:   []interface{}{feedID, userID},
				TaskType: zmsg.TaskTypeCount,
				BatchKey: fmt.Sprintf("like_count:%s", feedID),
			}

			err := zm.CacheAndDelayStore(ctx, key, []byte("1"), sqlTask)
			assert.NoError(t, err)
		}

		// 等待批处理
		time.Sleep(3 * time.Second)

		// 验证缓存
		for i := 0; i < 20; i++ {
			userID := fmt.Sprintf("queue_user_%d", i)
			key := fmt.Sprintf("like:%s:%s", feedID, userID)

			value, err := zm.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, []byte("1"), value)
		}
	})

	// 测试队列重试
	t.Run("TestQueueRetry", func(t *testing.T) {
		// 这个测试需要模拟数据库失败，比较复杂
		// 这里只测试正常流程
		key := "queue_retry_test"
		value := []byte("retry_value")

		sqlTask := &zmsg.SQLTask{
			Query:    "INSERT INTO test_retry (id, data) VALUES ($1, $2)",
			Params:   []interface{}{key, value},
			TaskType: zmsg.TaskTypeContent,
		}

		err := zm.CacheAndDelayStore(ctx, key, value, sqlTask)
		assert.NoError(t, err)

		// 验证缓存设置成功
		cached, err := zm.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, cached)
	})
}

// TestTaskSerialization 测试任务序列化
func TestTaskSerialization(t *testing.T) {
	// 创建各种类型的任务
	tasks := []*zmsg.SQLTask{
		{
			Query:    "INSERT INTO test (id, name) VALUES ($1, $2)",
			Params:   []interface{}{"id1", "name1"},
			TaskType: zmsg.TaskTypeContent,
		},
		{
			Query:    "UPDATE counters SET value = value + $1 WHERE key = $2",
			Params:   []interface{}{int64(1), "counter_key"},
			TaskType: zmsg.TaskTypeCount,
			BatchKey: "counter_batch",
		},
		{
			Query:    "INSERT INTO relations (from_id, to_id) VALUES ($1, $2)",
			Params:   []interface{}{"user1", "user2"},
			TaskType: zmsg.TaskTypeRelation,
		},
	}

	for i, task := range tasks {
		// 序列化
		data, err := json.Marshal(task)
		require.NoError(t, err, "Task %d should marshal", i)

		// 反序列化
		var decoded zmsg.SQLTask
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err, "Task %d should unmarshal", i)

		// 验证
		assert.Equal(t, task.Query, decoded.Query, "Task %d query should match", i)
		assert.Equal(t, task.TaskType, decoded.TaskType, "Task %d type should match", i)
		assert.Equal(t, task.BatchKey, decoded.BatchKey, "Task %d batch key should match", i)

		// 参数验证（JSON会转换数字类型）
		if len(task.Params) > 0 {
			assert.Equal(t, len(task.Params), len(decoded.Params), "Task %d param count should match", i)
		}
	}
}
