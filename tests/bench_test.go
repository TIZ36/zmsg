package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

func init() { testutil.InitEnv() }

// BenchmarkCacheOnly 缓存性能测试
func BenchmarkCacheOnly(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_cache_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))

		err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet 读取性能测试
func BenchmarkGet(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	// 准备测试数据
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))

		err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Hour))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		_, err := zm.Get(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkNextID ID生成性能测试
func BenchmarkNextID(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := zm.NextID(ctx, "bench")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAndStore 缓存并存储性能测试
func BenchmarkCacheAndStore(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	// 准备测试表
	setupTask := &zmsg.SQLTask{
		Query: `
            CREATE TABLE IF NOT EXISTS benchmark_data (
                id VARCHAR(255) PRIMARY KEY,
                data BYTEA,
                created_at TIMESTAMP DEFAULT NOW()
            )
        `,
	}
	zm.SQLExec(ctx, setupTask)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_store_%d", i)
		value := []byte(fmt.Sprintf("store_value_%d", i))

		sqlTask := &zmsg.SQLTask{
			Query:  "INSERT INTO benchmark_data (id, data) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			Params: []interface{}{key, value},
		}

		_, err := zm.CacheAndStore(ctx, key, value, sqlTask)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentOperations 并发操作性能测试
func BenchmarkConcurrentOperations(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	// 准备测试表
	setupTask := &zmsg.SQLTask{
		Query: `
            CREATE TABLE IF NOT EXISTS benchmark_concurrent (
                id VARCHAR(255) PRIMARY KEY,
                data BYTEA,
                counter INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        `,
	}
	zm.SQLExec(ctx, setupTask)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			key := fmt.Sprintf("concurrent_%d_%d", time.Now().UnixNano(), i)
			value := []byte(fmt.Sprintf("value_%d", i))

			sqlTask := &zmsg.SQLTask{
				Query:  "INSERT INTO benchmark_concurrent (id, data) VALUES ($1, $2) ON CONFLICT DO NOTHING",
				Params: []interface{}{key, value},
			}

			_, err := zm.CacheAndStore(ctx, key, value, sqlTask)
			if err != nil {
				b.Fatal(err)
			}

			// 读取验证
			_, err = zm.Get(ctx, key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkBatchCounter 批量计数器性能测试
func BenchmarkBatchCounter(b *testing.B) {
	ctx := context.Background()

	cfg := testutil.NewConfig()

	zm, err := zmsg.New(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer zm.Close()

	// 准备测试表
	setupTask := &zmsg.SQLTask{
		Query: `
            CREATE TABLE IF NOT EXISTS benchmark_counters (
                feed_id VARCHAR(255) PRIMARY KEY,
                like_count INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        `,
	}
	zm.SQLExec(ctx, setupTask)

	feedID := "bench_feed_counter"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		userID := fmt.Sprintf("bench_user_%d", i)
		key := fmt.Sprintf("like:%s:%s", feedID, userID)

		sqlTask := &zmsg.SQLTask{
			Query:    "INSERT INTO benchmark_likes (feed_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			Params:   []interface{}{feedID, userID},
			BatchKey: fmt.Sprintf("like_count:%s", feedID),
		}

		err := zm.CacheAndDelayStore(ctx, key, []byte("1"), sqlTask)
		if err != nil {
			b.Fatal(err)
		}
	}
}
