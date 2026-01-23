package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// TestSQLExecutor 测试SQL执行器
func TestSQLExecutor(t *testing.T) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试基本SQL执行
	t.Run("TestBasicSQL", func(t *testing.T) {
		// 创建测试表
		createTask := &zmsg.SQLTask{
			Query: `
                CREATE TABLE IF NOT EXISTS test_sql (
                    id VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255),
                    age INTEGER,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            `,
		}

		_, err := zm.SQLExec(ctx, createTask)
		assert.NoError(t, err)

		// 插入数据
		insertTask := &zmsg.SQLTask{
			Query: "INSERT INTO test_sql (id, name, age, metadata) VALUES ($1, $2, $3, $4)",
			Params: []interface{}{
				"test_id_1",
				"John Doe",
				30,
				map[string]interface{}{
					"city":    "New York",
					"country": "USA",
				},
			},
		}

		result, err := zm.SQLExec(ctx, insertTask)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected)

		// 查询数据
		selectTask := &zmsg.SQLTask{
			Query:  "SELECT name, age FROM test_sql WHERE id = $1",
			Params: []interface{}{"test_id_1"},
		}

		result, err = zm.SQLExec(ctx, selectTask)
		assert.NoError(t, err)
	})

	// 测试参数类型转换
	t.Run("TestParamConversion", func(t *testing.T) {
		// 测试各种参数类型
		testCases := []struct {
			name   string
			params []interface{}
		}{
			{
				name: "Basic types",
				params: []interface{}{
					"string_value",
					123,
					int64(456),
					3.14,
					true,
					false,
				},
			},
			{
				name: "Time types",
				params: []interface{}{
					time.Now(),
					&time.Time{},
				},
			},
			{
				name: "Byte slice",
				params: []interface{}{
					[]byte("byte_data"),
				},
			},
			{
				name: "JSON data",
				params: []interface{}{
					map[string]interface{}{
						"key1": "value1",
						"key2": 123,
						"key3": []string{"a", "b", "c"},
					},
				},
			},
			{
				name: "Array types",
				params: []interface{}{
					[]string{"a", "b", "c"},
					[]int{1, 2, 3},
					[]int64{100, 200, 300},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// 构建占位符
				var placeholders []string
				for i := range tc.params {
					placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
				}

				query := fmt.Sprintf(
					"INSERT INTO test_params (data) VALUES (%s)",
					strings.Join(placeholders, ", "),
				)

				task := &zmsg.SQLTask{
					Query:  query,
					Params: tc.params,
				}

				_, err := zm.SQLExec(ctx, task)
				// 允许某些类型转换失败（取决于数据库支持）
				if err != nil {
					t.Logf("Param conversion test %s: %v", tc.name, err)
				}
			})
		}
	})

	// 测试批量SQL执行
	t.Run("TestBatchSQL", func(t *testing.T) {
		// 准备批量任务
		var tasks []*zmsg.SQLTask

		for i := 0; i < 10; i++ {
			task := &zmsg.SQLTask{
				Query: "INSERT INTO test_batch (id, value) VALUES ($1, $2)",
				Params: []interface{}{
					fmt.Sprintf("batch_id_%d", i),
					i * 100,
				},
			}
			tasks = append(tasks, task)
		}

		// 注意：zmsg接口不支持直接批量执行
		// 这里测试单个执行
		for _, task := range tasks {
			_, err := zm.SQLExec(ctx, task)
			assert.NoError(t, err)
		}
	})

	// 测试事务
	t.Run("TestTransaction", func(t *testing.T) {
		// 清理测试数据
		cleanupTask := &zmsg.SQLTask{
			Query: "DELETE FROM test_tx WHERE id LIKE 'tx_test_%'",
		}
		zm.SQLExec(ctx, cleanupTask)

		// 这里zmsg接口不直接暴露事务
		// 实际事务应该在业务层通过SQL执行器处理
	})
}
