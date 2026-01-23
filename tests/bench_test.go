package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tiz36/zmsg/tests/testutil"
	"github.com/tiz36/zmsg/zmsg"
)

// ============ 全局 zmsg 实例（避免重复初始化）============

var (
	benchZmsg     zmsg.ZMsg
	benchZmsgOnce sync.Once
)

func getBenchZmsg() zmsg.ZMsg {
	benchZmsgOnce.Do(func() {
		cfg := testutil.NewConfig()
		cfg.Log.Level = "error" // 减少日志
		ctx := context.Background()
		zm, err := zmsg.New(ctx, cfg)
		if err != nil {
			panic(fmt.Sprintf("failed to create zmsg for benchmark: %v", err))
		}
		benchZmsg = zm
	})
	return benchZmsg
}

// ============ SQL 构建器基准测试（纯 CPU，无 IO）============

func BenchmarkSQL_Builder(b *testing.B) {
	b.Run("SQL_Basic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello")
		}
	})

	b.Run("SQL_OnConflict", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", "feed_1", "hello").
				OnConflict("id").
				DoUpdate("content")
		}
	})

	b.Run("Counter_Inc", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.Counter("feed_meta", "like_count").
				Inc(1).
				Where("id = ?", "feed_1").
				BatchKey("meta:feed_1").
				Build()
		}
	})

	b.Run("Table_Column_Counter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.Table("feed_meta").Column("like_count").Counter().
				Inc(1).
				Where("id = ?", "feed_1").
				BatchKey("meta:feed_1").
				Build()
		}
	})

	b.Run("Slice_Add", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.Slice("feed_meta", "tags").
				Add("golang").
				Where("id = ?", "feed_1").
				Build()
		}
	})

	b.Run("Map_Set", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = zmsg.Map("feed_meta", "extra").
				Set("key", "value").
				Where("id = ?", "feed_1").
				Build()
		}
	})
}

// ============ 缓存操作基准测试 ============

func BenchmarkCacheOnly(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()
	value := []byte(`{"id":"test","content":"benchmark data"}`)

	// 预热
	for i := 0; i < 100; i++ {
		_ = zm.CacheOnly(ctx, fmt.Sprintf("warmup_%d", i), value, zmsg.WithTTL(time.Minute))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_cache_%d", i)
		_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
	}
}

func BenchmarkCacheOnly_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()
	value := []byte(`{"id":"test","content":"benchmark data"}`)

	// 预热
	for i := 0; i < 100; i++ {
		_ = zm.CacheOnly(ctx, fmt.Sprintf("warmup_parallel_%d", i), value, zmsg.WithTTL(time.Minute))
	}

	var counter int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("bench_cache_parallel_%d", idx)
			_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预先写入数据
	key := "bench_get_key"
	value := []byte(`{"id":"test","content":"benchmark data"}`)
	_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Hour))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = zm.Get(ctx, key)
	}
}

func BenchmarkGet_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预先写入数据
	key := "bench_get_parallel_key"
	value := []byte(`{"id":"test","content":"benchmark data"}`)
	_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Hour))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = zm.Get(ctx, key)
		}
	})
}

// ============ ID 生成基准测试 ============

func BenchmarkNextID(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预热
	for i := 0; i < 10; i++ {
		_, _ = zm.NextID(ctx, "warmup")
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = zm.NextID(ctx, "bench")
	}
}

func BenchmarkNextID_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预热
	for i := 0; i < 10; i++ {
		_, _ = zm.NextID(ctx, "warmup")
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = zm.NextID(ctx, "bench")
		}
	})
}

// ============ 周期写入基准测试 ============

func BenchmarkCacheAndPeriodicStore(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预热
	for i := 0; i < 100; i++ {
		task := zmsg.Counter("feed_meta", "like_count").
			Inc(1).
			Where("id = ?", "warmup_feed").
			BatchKey("meta:warmup").
			Build()
		_ = zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("warmup_%d", i), nil, task)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		task := zmsg.Counter("feed_meta", "like_count").
			Inc(1).
			Where("id = ?", "feed_1").
			BatchKey("meta:feed_1").
			Build()
		_ = zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("periodic_%d", i), nil, task)
	}
}

func BenchmarkCacheAndPeriodicStore_Parallel(b *testing.B) {
	zm := getBenchZmsg()
	ctx := context.Background()

	// 预热
	for i := 0; i < 100; i++ {
		task := zmsg.Counter("feed_meta", "like_count").
			Inc(1).
			Where("id = ?", "warmup_feed").
			BatchKey("meta:warmup").
			Build()
		_ = zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("warmup_parallel_%d", i), nil, task)
	}

	var counter int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1)
			task := zmsg.Counter("feed_meta", "like_count").
				Inc(1).
				Where("id = ?", "feed_1").
				BatchKey("meta:feed_1").
				Build()
			_ = zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("periodic_parallel_%d", idx), nil, task)
		}
	})
}

// ============ 吞吐量测试 ============

func TestThroughput_Write(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	value := []byte(`{"id":"test","content":"throughput test data"}`)
	totalOps := 100000
	concurrency := 100

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	start := time.Now()

	opsPerWorker := totalOps / concurrency
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("throughput_w%d_%d", workerID, i)
				err := zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("=== Write Throughput Test ===")
	t.Logf("Total operations: %d", totalOps)
	t.Logf("Concurrency: %d", concurrency)
	t.Logf("Success: %d, Errors: %d", successCount, errorCount)
	t.Logf("Duration: %v", elapsed)
	t.Logf("Throughput: %.2f ops/sec", float64(successCount)/elapsed.Seconds())
}

func TestThroughput_Read(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	// 预先写入数据
	keys := make([]string, 1000)
	value := []byte(`{"id":"test","content":"throughput test data"}`)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("throughput_read_%d", i)
		_ = zm.CacheOnly(ctx, keys[i], value, zmsg.WithTTL(time.Hour))
	}

	totalOps := 100000
	concurrency := 100

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	start := time.Now()

	opsPerWorker := totalOps / concurrency
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := keys[i%len(keys)]
				_, err := zm.Get(ctx, key)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("=== Read Throughput Test ===")
	t.Logf("Total operations: %d", totalOps)
	t.Logf("Concurrency: %d", concurrency)
	t.Logf("Success: %d, Errors: %d", successCount, errorCount)
	t.Logf("Duration: %v", elapsed)
	t.Logf("Throughput: %.2f ops/sec", float64(successCount)/elapsed.Seconds())
}

func TestThroughput_PeriodicStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	totalOps := 50000
	concurrency := 100

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	start := time.Now()

	opsPerWorker := totalOps / concurrency
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				feedID := fmt.Sprintf("feed_%d", workerID%10)
				task := zmsg.Table("feed_meta").Column("like_count").Counter().
					Inc(1).
					Where("id = ?", feedID).
					BatchKey("meta:" + feedID).
					Build()

				key := fmt.Sprintf("periodic_throughput_w%d_%d", workerID, i)
				err := zm.CacheAndPeriodicStore(ctx, key, nil, task)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("=== PeriodicStore Throughput Test ===")
	t.Logf("Total operations: %d", totalOps)
	t.Logf("Concurrency: %d", concurrency)
	t.Logf("Success: %d, Errors: %d", successCount, errorCount)
	t.Logf("Duration: %v", elapsed)
	t.Logf("Throughput: %.2f ops/sec", float64(successCount)/elapsed.Seconds())
}

// ============ 延迟测试 ============

func TestLatency_CacheOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	value := []byte(`{"id":"test","content":"latency test"}`)
	samples := 10000
	latencies := make([]time.Duration, samples)

	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("latency_%d", i)
		start := time.Now()
		_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Minute))
		latencies[i] = time.Since(start)
	}

	printLatencyStats(t, "CacheOnly", latencies)
}

func TestLatency_Get(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	// 预写入
	key := "latency_get_key"
	value := []byte(`{"id":"test","content":"latency test"}`)
	_ = zm.CacheOnly(ctx, key, value, zmsg.WithTTL(time.Hour))

	samples := 10000
	latencies := make([]time.Duration, samples)

	for i := 0; i < samples; i++ {
		start := time.Now()
		_, _ = zm.Get(ctx, key)
		latencies[i] = time.Since(start)
	}

	printLatencyStats(t, "Get", latencies)
}

func printLatencyStats(t *testing.T, name string, latencies []time.Duration) {
	// 计算统计信息
	var total time.Duration
	var min, max time.Duration = latencies[0], latencies[0]

	for _, lat := range latencies {
		total += lat
		if lat < min {
			min = lat
		}
		if lat > max {
			max = lat
		}
	}

	avg := total / time.Duration(len(latencies))

	// 排序计算百分位数
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sortDurations(sorted)

	p50 := sorted[len(sorted)*50/100]
	p95 := sorted[len(sorted)*95/100]
	p99 := sorted[len(sorted)*99/100]

	t.Logf("=== %s Latency Test ===", name)
	t.Logf("Samples: %d", len(latencies))
	t.Logf("Min: %v, Max: %v, Avg: %v", min, max, avg)
	t.Logf("P50: %v, P95: %v, P99: %v", p50, p95, p99)
}

func sortDurations(d []time.Duration) {
	for i := 0; i < len(d)-1; i++ {
		for j := i + 1; j < len(d); j++ {
			if d[i] > d[j] {
				d[i], d[j] = d[j], d[i]
			}
		}
	}
}

// ============ 内存聚合测试 ============

func TestAggregation_Counter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping aggregation test in short mode")
	}

	zm := getBenchZmsg()
	ctx := context.Background()

	feedID := fmt.Sprintf("agg_test_%d", time.Now().UnixNano())

	// 模拟高并发计数器累加
	totalIncs := 1000
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < totalIncs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task := zmsg.Table("feed_meta").Column("like_count").Counter().
				Inc(1).
				Where("id = ?", feedID).
				BatchKey("meta:" + feedID).
				Build()

			_ = zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("agg_%s_%d", feedID, idx), nil, task)
		}(i)
	}

	wg.Wait()
	submitDuration := time.Since(start)

	t.Logf("=== Counter Aggregation Test ===")
	t.Logf("Total increments: %d", totalIncs)
	t.Logf("Submit duration: %v", submitDuration)
	t.Logf("Submit throughput: %.2f ops/sec", float64(totalIncs)/submitDuration.Seconds())
	t.Logf("Expected: all increments aggregated to single UPDATE with +%d", totalIncs)
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
