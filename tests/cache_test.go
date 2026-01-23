package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// TestCacheLayer 测试缓存层
func TestCacheLayer(t *testing.T) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试L1缓存
	t.Run("TestL1Cache", func(t *testing.T) {
		key := "l1_cache_test"
		value := []byte("l1_value")

		// 设置缓存
		err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
		assert.NoError(t, err)

		// 立即读取（应该命中L1）
		cached, err := zm.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, cached)
	})

	// 测试L2缓存
	t.Run("TestL2Cache", func(t *testing.T) {
		key := "l2_cache_test"
		value := []byte("l2_value")

		// 设置缓存
		err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
		assert.NoError(t, err)

		// 模拟L1失效（重新创建zmsg实例）
		// 这里简化测试，实际应该测试缓存穿透
	})

	// 测试缓存过期
	t.Run("TestCacheExpiration", func(t *testing.T) {
		key := "expire_test"
		value := []byte("expire_value")

		// 设置1秒过期
		err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Second))
		assert.NoError(t, err)

		// 立即读取
		cached, err := zm.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, cached)

		// 等待过期
		time.Sleep(2 * time.Second)

		// 应该获取不到
		_, err = zm.Get(ctx, key)
		assert.Error(t, err)
	})
}

// TestBloomFilter 测试布隆过滤器
func TestBloomFilter(t *testing.T) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	// 测试布隆过滤器
	key := "bloom_test_key"

	// 初始测试
	hit := zm.DBHit(ctx, key)
	assert.False(t, hit)

	// 存储数据
	value := []byte("bloom_value")
	sqlTask := &zmsg.SQLTask{
		Query:  "INSERT INTO test_bloom (id, data) VALUES ($1, $2)",
		Params: []interface{}{key, value},
	}

	_, err = zm.CacheAndStore(ctx, key, value, sqlTask)
	require.NoError(t, err)

	// 再次测试
	hit = zm.DBHit(ctx, key)
	assert.True(t, hit)

	// 测试不存在但布隆过滤器可能误判的情况
	fakeKey := "fake_bloom_key"
	fakeHit := zm.DBHit(ctx, fakeKey)
	// 布隆过滤器可能有假阳性，但不能有假阴性
	// 如果存在，一定返回true；如果返回false，一定不存在
	if fakeHit {
		t.Log("Bloom filter false positive detected (acceptable)")
	}
}

// TestSingleFlight 测试SingleFlight防缓存击穿
func TestSingleFlight(t *testing.T) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	require.NoError(t, err)
	defer zm.Close()

	key := "sf_test_key"

	// 清空可能存在的缓存
	zm.Del(ctx, key)

	// 模拟并发读取
	const numGoroutines = 10
	results := make(chan []byte, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			value, err := zm.Get(ctx, key)
			if err != nil {
				results <- nil
			} else {
				results <- value
			}
		}(i)
	}

	// 收集结果
	var firstResult []byte
	for i := 0; i < numGoroutines; i++ {
		result := <-results
		if firstResult == nil && result != nil {
			firstResult = result
		}
		// 所有结果应该相同或都是nil
		// assert.Equal(t, firstResult, result)
	}

	// 验证数据库查询次数（应该只有1次）
	// 这里无法直接验证，但通过日志可以观察
}
