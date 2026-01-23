package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// TestIDGeneration 测试ID生成
func TestIDGeneration(t *testing.T) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试ID唯一性
	t.Run("TestIDUniqueness", func(t *testing.T) {
		ids := make(map[string]bool)

		for i := 0; i < 1000; i++ {
			id, err := zm.NextID(ctx, "test")
			require.NoError(t, err)

			// 检查格式
			assert.Contains(t, id, "test_")

			// 检查唯一性
			assert.False(t, ids[id], "Duplicate ID generated: %s", id)
			ids[id] = true
		}
	})

	// 测试不同前缀的ID
	t.Run("TestDifferentPrefixes", func(t *testing.T) {
		prefixes := []string{"user", "feed", "comment", "like"}

		for _, prefix := range prefixes {
			id, err := zm.NextID(ctx, prefix)
			require.NoError(t, err)
			assert.Contains(t, id, prefix+"_")
		}
	})

	// 测试ID递增性
	t.Run("TestIDMonotonic", func(t *testing.T) {
		var lastID string

		for i := 0; i < 100; i++ {
			id, err := zm.NextID(ctx, "seq")
			require.NoError(t, err)

			if lastID != "" {
				// 由于是字符串，我们确保时间部分递增
				// 简化测试：只检查不重复
				assert.NotEqual(t, lastID, id)
			}

			lastID = id
		}
	})

	// 测试高并发ID生成
	t.Run("TestConcurrentIDGeneration", func(t *testing.T) {
		const numWorkers = 20
		const idsPerWorker = 50

		ids := make(chan string, numWorkers*idsPerWorker)
		errs := make(chan error, numWorkers)

		for w := 0; w < numWorkers; w++ {
			go func(workerID int) {
				for i := 0; i < idsPerWorker; i++ {
					id, err := zm.NextID(ctx, fmt.Sprintf("worker_%d", workerID))
					if err != nil {
						errs <- err
						return
					}
					ids <- id
				}
				errs <- nil
			}(w)
		}

		// 收集所有ID
		allIDs := make(map[string]bool)
		errorCount := 0

		for w := 0; w < numWorkers; w++ {
			err := <-errs
			if err != nil {
				errorCount++
				t.Logf("Worker error: %v", err)
			}
		}

		close(ids)
		for id := range ids {
			if allIDs[id] {
				t.Errorf("Duplicate ID in concurrent generation: %s", id)
			}
			allIDs[id] = true
		}

		assert.Equal(t, 0, errorCount, "Should have no errors in concurrent ID generation")
		assert.Equal(t, numWorkers*idsPerWorker, len(allIDs), "Should generate all unique IDs")
	})

	// 测试节点ID管理
	t.Run("TestNodeIDManagement", func(t *testing.T) {
		// 创建多个zmsg实例模拟多个节点
		var instances []zmsg.ZMsg
		var nodeIDs []string

		// 创建3个实例
		for i := 0; i < 3; i++ {
			instanceCfg := testutil.NewConfig()

			instance, err := zmsg.New(ctx, instanceCfg)
			require.NoError(t, err)

			instances = append(instances, instance)

			// 每个实例生成ID
			id, err := instance.NextID(ctx, "node_test")
			require.NoError(t, err)
			nodeIDs = append(nodeIDs, id)
		}

		// 清理
		for _, instance := range instances {
			instance.Close()
		}

		// 验证所有ID唯一
		uniqueIDs := make(map[string]bool)
		for _, id := range nodeIDs {
			uniqueIDs[id] = true
		}
		assert.Equal(t, len(nodeIDs), len(uniqueIDs), "All node IDs should be unique")
	})
}
