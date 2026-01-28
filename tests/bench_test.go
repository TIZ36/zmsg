package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	zmsg "github.com/tiz36/zmsg/core"
	"github.com/tiz36/zmsg/tests/testutil"
)

// ============ 全局 zmsg 实例（避免重复初始化）============

var (
	benchZmsg     zmsg.ZMsg
	benchZmsgOnce sync.Once
)

func getBenchZmsg() zmsg.ZMsg {
	benchZmsgOnce.Do(func() {
		// 清理 Redis 队列，防止旧任务干扰
		testutil.CleanupRedis(nil)

		cfg := testutil.NewConfig()
		cfg.Log.Level = "error" // 减少日志
		ctx := context.Background()
		zm, err := zmsg.New(ctx, cfg)
		if err != nil {
			panic(fmt.Sprintf("failed to create zmsg for benchmark: %v", err))
		}
		benchZmsg = zm

		// 清理数据库表
		testutil.CleanupAllTables(nil, zm)
	})
	return benchZmsg
}

// ============ SQL 构建器基准测试（纯 CPU，无 IO）============

func BenchmarkSQL_Builder(b *testing.B) {
	zm := getBenchZmsg()

	b.Run("Table_Save_Build", func(b *testing.B) {
		b.ReportAllocs()
		user := struct {
			ID   string `db:"id,pk"`
			Name string `db:"name"`
		}{ID: "1", Name: "test"}
		for i := 0; i < b.N; i++ {
			_ = zm.Table("users").CacheKey("1").Save(user)
		}
	})

	b.Run("Counter_Do_Build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zm.Table("feed_meta").
				CacheKey("meta:1").
				PeriodicCount().
				UpdateColumn().
				Column("like_count").
				Do(zmsg.Add(), 1)
		}
	})
}

// ============ 缓存操作基准测试 ============

func BenchmarkCache_Save(b *testing.B) {
	zm := getBenchZmsg()
	user := struct {
		ID   string `db:"id,pk"`
		Name string `db:"name"`
	}{ID: "1", Name: "test"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_save_%d", i)
		user.ID = key
		_ = zm.Table("users").CacheKey(key).Save(user)
	}
}

func BenchmarkCache_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	user := struct {
		ID   string `db:"id,pk"`
		Name string `db:"name"`
	}{Name: "test"}

	var counter int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1)
			id := fmt.Sprintf("bench_save_%d", idx)
			user.ID = id
			_ = zm.Table("users").CacheKey(id).Save(user)
		}
	})
}

func BenchmarkNextID(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, _ = zm.NextID(ctx, "bench")
	}
}

func BenchmarkNextID_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = zm.NextID(ctx, "bench")
		}
	})
}

// ============ 周期写入基准测试 ============

func BenchmarkPeriodic(b *testing.B) {
	zm := getBenchZmsg()

	b.Run("Counter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("periodic_%d", i)
			_ = zm.Table("feed_meta").
				CacheKey(key).
				PeriodicCount().
				UpdateColumn().
				Column("like_count").
				Do(zmsg.Add(), 1)
		}
	})
}

// ============ 吞吐量测试 ============

func BenchmarkThroughput(b *testing.B) {
	zm := getBenchZmsg()

	b.Run("Save", func(b *testing.B) {
		b.ReportAllocs()
		user := struct {
			ID   string `db:"id,pk"`
			Name string `db:"name"`
		}{ID: "1", Name: "test"}
		for i := 0; i < b.N; i++ {
			user.ID = fmt.Sprintf("t_%d", i)
			_ = zm.Table("users").CacheKey(user.ID).Save(user)
		}
	})

	b.Run("Query", func(b *testing.B) {
		b.ReportAllocs()
		var result map[string]any
		for i := 0; i < b.N; i++ {
			_ = zm.Table("users").CacheKey("1").Query(&result)
		}
	})
}

// ============ 延迟测试 ============

func TestLatency(t *testing.T) {
	// 简单延迟统计在基准测试中覆盖
}

// ============ 吞吐量对比基准测试 ============

func BenchmarkThroughput_Save_Sync(b *testing.B) {
	zm := getBenchZmsg()
	table := zm.Table("feeds")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("bench_save_%d", i)
			_ = table.CacheKey(id).Save(struct {
				ID      string `db:"id,pk"`
				Content string `db:"content"`
			}{ID: id, Content: "bench"})
			i++
		}
	})
}

func BenchmarkThroughput_Save_Periodic(b *testing.B) {
	zm := getBenchZmsg()
	table := zm.Table("feeds").PeriodicOverride()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("bench_per_%d", i)
			_ = table.CacheKey(id).Save(struct {
				ID      string `db:"id,pk"`
				Content string `db:"content"`
			}{ID: id, Content: "bench"})
			i++
		}
	})
}

func BenchmarkThroughput_Counter_Periodic(b *testing.B) {
	zm := getBenchZmsg()
	table := zm.Table("feed_meta").PeriodicCount()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = table.CacheKey("bench_counter").
				UpdateColumn().
				Column("like_count").
				Do(zmsg.Add(), 1)
		}
	})
}

// ============ 序列化（分布式锁）开销测试 ============

func BenchmarkSerialization_Overhead(b *testing.B) {
	zm := getBenchZmsg()
	table := zm.Table("feeds")

	b.Run("NoSerialize", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = table.CacheKey("lock_test").Save(struct {
					ID      string `db:"id,pk"`
					Content string `db:"content"`
				}{ID: "lock_test", Content: "val"})
			}
		})
	})

	b.Run("WithSerialize", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = table.CacheKey("lock_test").
					Serialize("lock_test").
					Save(struct {
						ID      string `db:"id,pk"`
						Content string `db:"content"`
					}{ID: "lock_test", Content: "val"})
			}
		})
	})
}

// ============ JSON 序列化性能 ============

func BenchmarkJSON_Marshal(b *testing.B) {
	data := map[string]interface{}{
		"id":         "feed_12345",
		"content":    "This is a test feed content",
		"like_count": 100,
		"tags":       []string{"golang", "redis", "postgres"},
		"created_at": time.Now().Format(time.RFC3339),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(data)
	}
}

func BenchmarkJSON_Unmarshal(b *testing.B) {
	data := []byte(`{"id":"feed_12345","content":"This is a test feed content","like_count":100,"tags":["golang","redis","postgres"],"created_at":"2024-01-01T00:00:00Z"}`)

	var result map[string]interface{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = json.Unmarshal(data, &result)
	}
}
