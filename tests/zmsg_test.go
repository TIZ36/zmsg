package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// ZMsgTestSuite 测试套件
type ZMsgTestSuite struct {
	suite.Suite
	ctx context.Context
	zm  zmsg.ZMsg
}

// SetupSuite 测试套件初始化
func (s *ZMsgTestSuite) SetupSuite() {
	s.ctx = context.Background()

	cfg := testutil.NewConfig()
	cfg.Queue.Addr = "localhost:6380"
	cfg.Batch.Size = 10 // 测试用小批量

	var err error
	s.zm, err = zmsg.New(s.ctx, cfg)
	require.NoError(s.T(), err)
}

// TearDownSuite 测试套件清理
func (s *ZMsgTestSuite) TearDownSuite() {
	if s.zm != nil {
		s.zm.Close()
	}
}

// TestCacheOnly 测试CacheOnly
func (s *ZMsgTestSuite) TestCacheOnly() {
	key := "test_cache_only"
	value := []byte("cache_only_value")

	// 设置缓存
	err := s.zm.CacheOnly(s.ctx, key, value, zmsg.WithTTL(time.Minute))
	assert.NoError(s.T(), err)

	// 验证缓存
	cached, err := s.zm.Get(s.ctx, key)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), value, cached)

	// 删除缓存
	err = s.zm.Del(s.ctx, key)
	assert.NoError(s.T(), err)

	// 验证删除
	_, err = s.zm.Get(s.ctx, key)
	assert.Error(s.T(), err)
}

// TestCacheAndStore 测试CacheAndStore
func (s *ZMsgTestSuite) TestCacheAndStore() {
	key := "test_cache_and_store"
	value := []byte(`{"name": "test", "value": 123}`)

	sqlTask := &zmsg.SQLTask{
		Query: `
            INSERT INTO test_data (id, data) 
            VALUES ($1, $2) 
            ON CONFLICT (id) DO UPDATE SET data = $2
        `,
		Params: []interface{}{key, value},
	}

	// 缓存并存储
	storedID, err := s.zm.CacheAndStore(s.ctx, key, value, sqlTask)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), key, storedID)

	// 验证缓存
	cached, err := s.zm.Get(s.ctx, key)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), value, cached)
}

// TestCacheAndDelayStore 测试延迟存储
func (s *ZMsgTestSuite) TestCacheAndDelayStore() {
	key := "test_delay_store"
	value := []byte("delay_store_value")

	sqlTask := &zmsg.SQLTask{
		Query:    "INSERT INTO test_queue (id, data) VALUES ($1, $2)",
		Params:   []interface{}{key, value},
		TaskType: zmsg.TaskTypeContent,
	}

	// 延迟存储
	err := s.zm.CacheAndDelayStore(s.ctx, key, value, sqlTask)
	assert.NoError(s.T(), err)

	// 立即验证缓存
	cached, err := s.zm.Get(s.ctx, key)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), value, cached)

	// 等待队列处理
	time.Sleep(2 * time.Second)
}

// TestDelStore 测试删除并存储
func (s *ZMsgTestSuite) TestDelStore() {
	key := "test_del_store"
	value := []byte("del_store_value")

	// 先设置数据
	sqlTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_data (id, data) VALUES ($1, $2)",
		Params: []interface{}{key, value},
	}

	_, err := s.zm.CacheAndStore(s.ctx, key, value, sqlTask)
	require.NoError(s.T(), err)

	// 删除SQL
	delTask := &zmsg.SQLTask{
		Query:  "DELETE FROM test_data WHERE id = $1",
		Params: []interface{}{key},
	}

	// 删除并存储
	err = s.zm.DelStore(s.ctx, key, delTask)
	assert.NoError(s.T(), err)

	// 验证删除
	_, err = s.zm.Get(s.ctx, key)
	assert.Error(s.T(), err)
}

// TestUpdateStore 测试更新并存储
func (s *ZMsgTestSuite) TestUpdateStore() {
	key := "test_update_store"
	oldValue := []byte("old_value")
	newValue := []byte("new_value")

	// 先插入旧数据
	insertTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_data (id, data) VALUES ($1, $2)",
		Params: []interface{}{key, oldValue},
	}

	_, err := s.zm.CacheAndStore(s.ctx, key, oldValue, insertTask)
	require.NoError(s.T(), err)

	// 更新SQL
	updateTask := &zmsg.SQLTask{
		Query:  "UPDATE test_data SET data = $1 WHERE id = $2",
		Params: []interface{}{newValue, key},
	}

	// 更新并存储
	err = s.zm.UpdateStore(s.ctx, key, newValue, updateTask)
	assert.NoError(s.T(), err)

	// 验证更新
	updated, err := s.zm.Get(s.ctx, key)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), newValue, updated)
}

// TestNextID 测试ID生成
func (s *ZMsgTestSuite) TestNextID() {
	// 生成多个ID
	ids := make(map[string]bool)

	for i := 0; i < 100; i++ {
		id, err := s.zm.NextID(s.ctx, "test")
		assert.NoError(s.T(), err)
		assert.Contains(s.T(), id, "test_")

		// 确保ID唯一
		assert.False(s.T(), ids[id])
		ids[id] = true
	}
}

// TestDBHit 测试布隆过滤器
func (s *ZMsgTestSuite) TestDBHit() {
	key := "test_bloom"

	// 初始应该不在布隆过滤器中
	hit := s.zm.DBHit(s.ctx, key)
	assert.False(s.T(), hit)

	// 存储后应该在布隆过滤器中
	value := []byte("bloom_test")
	sqlTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_data (id, data) VALUES ($1, $2)",
		Params: []interface{}{key, value},
	}

	_, err := s.zm.CacheAndStore(s.ctx, key, value, sqlTask)
	require.NoError(s.T(), err)

	// 现在应该在布隆过滤器中
	hit = s.zm.DBHit(s.ctx, key)
	assert.True(s.T(), hit)
}

// TestBatchOperation 测试批处理操作
func (s *ZMsgTestSuite) TestBatchOperation() {
	batch := s.zm.Batch()

	// 添加多个操作
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		value := []byte(fmt.Sprintf("batch_value_%d", i))
		batch.CacheOnly(key, value, zmsg.WithTTL(time.Minute))
	}

	assert.Equal(s.T(), 5, batch.Size())

	// 执行批处理
	err := batch.Execute(s.ctx)
	assert.NoError(s.T(), err)

	// 验证所有数据都已缓存
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		value, err := s.zm.Get(s.ctx, key)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), []byte(fmt.Sprintf("batch_value_%d", i)), value)
	}
}

// TestSQLExec 测试SQL执行
func (s *ZMsgTestSuite) TestSQLExec() {
	// 创建测试表
	createTask := &zmsg.SQLTask{
		Query: `
            CREATE TABLE IF NOT EXISTS test_sql_exec (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                value INTEGER
            )
        `,
	}

	_, err := s.zm.SQLExec(s.ctx, createTask)
	assert.NoError(s.T(), err)

	// 插入数据
	insertTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_sql_exec (id, name, value) VALUES ($1, $2, $3)",
		Params: []interface{}{"test_id", "test_name", 123},
	}

	result, err := s.zm.SQLExec(s.ctx, insertTask)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), result.RowsAffected)

	// 查询数据
	selectTask := &zmsg.SQLTask{
		Query:  "SELECT COUNT(*) FROM test_sql_exec WHERE value > $1",
		Params: []interface{}{100},
	}

	result, err = s.zm.SQLExec(s.ctx, selectTask)
	assert.NoError(s.T(), err)
}

// TestConcurrentOperations 测试并发操作
func (s *ZMsgTestSuite) TestConcurrentOperations() {
	const numWorkers = 10
	const numOps = 100

	done := make(chan bool, numWorkers)
	errCh := make(chan error, numWorkers*numOps)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("concurrent_%d_%d", workerID, i)
				value := []byte(fmt.Sprintf("value_%d_%d", workerID, i))

				sqlTask := &zmsg.SQLTask{
					Query:  "INSERT INTO test_concurrent (id, data) VALUES ($1, $2) ON CONFLICT DO NOTHING",
					Params: []interface{}{key, value},
				}

				_, err := s.zm.CacheAndStore(s.ctx, key, value, sqlTask)
				if err != nil {
					errCh <- err
				}

				// 读取验证
				cached, err := s.zm.Get(s.ctx, key)
				if err != nil || !assert.Equal(s.T(), value, cached) {
					errCh <- fmt.Errorf("read failed for key %s: %v", key, err)
				}
			}
			done <- true
		}(w)
	}

	// 等待所有goroutine完成
	for w := 0; w < numWorkers; w++ {
		<-done
	}

	close(errCh)

	// 检查错误
	for err := range errCh {
		assert.NoError(s.T(), err)
	}
}

// TestCounterBatch 测试计数器批处理
func (s *ZMsgTestSuite) TestCounterBatch() {
	feedID := "test_feed_counter"

	// 模拟多个点赞
	for i := 0; i < 50; i++ {
		userID := fmt.Sprintf("user_%d", i)
		key := fmt.Sprintf("like:%s:%s", feedID, userID)

		sqlTask := &zmsg.SQLTask{
			Query:    "INSERT INTO feed_likes (feed_id, user_id) VALUES ($1, $2)",
			Params:   []interface{}{feedID, userID},
			TaskType: zmsg.TaskTypeCount,
			BatchKey: fmt.Sprintf("like_count:%s", feedID),
		}

		err := s.zm.CacheAndDelayStore(s.ctx, key, []byte("1"), sqlTask)
		assert.NoError(s.T(), err)
	}

	// 等待批处理
	time.Sleep(3 * time.Second)
}

// RunTests 运行所有测试
func TestZMsgSuite(t *testing.T) {
	suite.Run(t, new(ZMsgTestSuite))
}
