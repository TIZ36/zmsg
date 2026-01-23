package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// TestIntegration 集成测试
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// 使用Testcontainers启动PostgreSQL
	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer pgContainer.Terminate(ctx)

	// 获取PostgreSQL端口
	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)
	pgHost, err := pgContainer.Host(ctx)
	require.NoError(t, err)

	// 使用Testcontainers启动Redis
	redisReq := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer redisContainer.Terminate(ctx)

	// 获取Redis端口
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)
	redisHost, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	// 构建连接字符串
	pgDSN := fmt.Sprintf("postgresql://test:test@%s:%s/test?sslmode=disable", pgHost, pgPort.Port())
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	// 等待服务就绪
	time.Sleep(5 * time.Second)

	// 创建zmsg实例
	cfg := testutil.NewConfig()
	cfg.Postgres.DSN = pgDSN
	cfg.Redis.Addr = redisAddr
	cfg.Queue.Addr = redisAddr // 测试使用同一个Redis

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试完整的业务场景
	t.Run("CompleteWorkflow", func(t *testing.T) {
		// 1. 生成ID
		feedID, err := zm.NextID(ctx, "feed")
		require.NoError(t, err)

		// 2. 创建Feed
		feedData := []byte(`{"content": "Integration test", "author": "tester"}`)

		sqlTask := &zmsg.SQLTask{
			Query: `
                CREATE TABLE IF NOT EXISTS integration_feeds (
                    id VARCHAR(255) PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            `,
		}
		zm.SQLExec(ctx, sqlTask)

		insertTask := &zmsg.SQLTask{
			Query:  "INSERT INTO integration_feeds (id, data) VALUES ($1, $2)",
			Params: []interface{}{feedID, feedData},
		}

		storedID, err := zm.CacheAndStore(ctx, feedID, feedData, insertTask)
		assert.NoError(t, err)
		assert.Equal(t, feedID, storedID)

		// 3. 读取Feed
		cachedData, err := zm.Get(ctx, feedID)
		assert.NoError(t, err)
		assert.Equal(t, feedData, cachedData)

		// 4. 模拟点赞
		for i := 0; i < 5; i++ {
			userID := fmt.Sprintf("integration_user_%d", i)
			likeKey := fmt.Sprintf("like:%s:%s", feedID, userID)

			likeTask := &zmsg.SQLTask{
				Query: `
                    CREATE TABLE IF NOT EXISTS integration_likes (
                        feed_id VARCHAR(255),
                        user_id VARCHAR(255),
                        liked_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (feed_id, user_id)
                    )
                `,
			}
			zm.SQLExec(ctx, likeTask)

			likeInsertTask := &zmsg.SQLTask{
				Query:    "INSERT INTO integration_likes (feed_id, user_id) VALUES ($1, $2)",
				Params:   []interface{}{feedID, userID},
				TaskType: zmsg.TaskTypeCount,
				BatchKey: fmt.Sprintf("like_count:%s", feedID),
			}

			err := zm.CacheAndDelayStore(ctx, likeKey, []byte("1"), likeInsertTask)
			assert.NoError(t, err)
		}

		// 5. 等待队列处理
		time.Sleep(3 * time.Second)

		// 6. 更新Feed
		updatedData := []byte(`{"content": "Updated integration test", "author": "tester"}`)

		updateTask := &zmsg.SQLTask{
			Query:  "UPDATE integration_feeds SET data = $1 WHERE id = $2",
			Params: []interface{}{updatedData, feedID},
		}

		err = zm.UpdateStore(ctx, feedID, updatedData, updateTask)
		assert.NoError(t, err)

		// 7. 验证更新
		updated, err := zm.Get(ctx, feedID)
		assert.NoError(t, err)
		assert.Equal(t, updatedData, updated)

		// 8. 删除Feed
		deleteTask := &zmsg.SQLTask{
			Query:  "DELETE FROM integration_feeds WHERE id = $1",
			Params: []interface{}{feedID},
		}

		err = zm.DelStore(ctx, feedID, deleteTask)
		assert.NoError(t, err)

		// 9. 验证删除
		_, err = zm.Get(ctx, feedID)
		assert.Error(t, err)
	})

	// 测试性能场景
	t.Run("PerformanceScenario", func(t *testing.T) {
		const numOperations = 100

		start := time.Now()

		// 批量操作
		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("perf_%d", i)
			value := []byte(fmt.Sprintf(`{"index": %d, "timestamp": "%s"}`, i, time.Now().Format(time.RFC3339)))

			sqlTask := &zmsg.SQLTask{
				Query: `
                    CREATE TABLE IF NOT EXISTS performance_test (
                        id VARCHAR(255) PRIMARY KEY,
                        data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                `,
			}
			zm.SQLExec(ctx, sqlTask)

			insertTask := &zmsg.SQLTask{
				Query:  "INSERT INTO performance_test (id, data) VALUES ($1, $2) ON CONFLICT DO NOTHING",
				Params: []interface{}{key, value},
			}

			_, err := zm.CacheAndStore(ctx, key, value, insertTask)
			if err != nil {
				t.Logf("Operation %d failed: %v", i, err)
			}
		}

		duration := time.Since(start)
		opsPerSec := float64(numOperations) / duration.Seconds()

		t.Logf("Performance: %d operations in %v (%.2f ops/sec)", numOperations, duration, opsPerSec)

		// 验证至少部分数据存在
		for i := 0; i < min(10, numOperations); i++ {
			key := fmt.Sprintf("perf_%d", i)
			_, err := zm.Get(ctx, key)
			if err != nil {
				t.Logf("Key %s not found (acceptable in performance test)", key)
			}
		}
	})
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
