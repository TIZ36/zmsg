package zmsg

import (
	"sync/atomic"
	"time"

	"github.com/tiz36/zmsg/internal/queue"
)

type metrics struct {
	l1Hits int64
	l2Hits int64
	dbHits int64
	misses int64

	cacheWriteFailures int64
	cacheRollbacks     int64
	cacheCompensations int64

	queueEnqueueFailures int64
	queueEnqueueSuccess  int64

	queryCount         int64
	queryDurationNanos int64
	writeCount         int64
	writeDurationNanos int64

	// Prometheus Exporter
	exporter *MetricsExporter
}

func newMetrics() *metrics {
	return &metrics{
		exporter: NewMetricsExporter("zmsg"),
	}
}

func (m *metrics) recordCacheHit(level string) {
	switch level {
	case "l1":
		atomic.AddInt64(&m.l1Hits, 1)
	case "l2":
		atomic.AddInt64(&m.l2Hits, 1)
	case "db":
		atomic.AddInt64(&m.dbHits, 1)
	}
	if m.exporter != nil {
		m.exporter.cacheHits.WithLabelValues(level).Inc()
	}
}

func (m *metrics) recordCacheMiss() {
	atomic.AddInt64(&m.misses, 1)
	if m.exporter != nil {
		m.exporter.cacheMisses.Inc()
	}
}

// recordWriteLatency records latency in nanoseconds but updates histogram in seconds
func (m *metrics) recordWriteLatency(d time.Duration) {
	atomic.AddInt64(&m.writeCount, 1)
	atomic.AddInt64(&m.writeDurationNanos, d.Nanoseconds())
	if m.exporter != nil {
		m.exporter.writeLatency.Observe(d.Seconds())
	}
}

func (m *metrics) recordQueryLatency(d time.Duration) {
	atomic.AddInt64(&m.queryCount, 1)
	atomic.AddInt64(&m.queryDurationNanos, d.Nanoseconds())
	if m.exporter != nil {
		m.exporter.queryLatency.Observe(d.Seconds())
	}
}

// Exporter exposes the Prometheus exporter
func (m *metrics) Exporter() *MetricsExporter {
	return m.exporter
}

func (m *metrics) recordCacheWriteFailure() {
	atomic.AddInt64(&m.cacheWriteFailures, 1)
}

func (m *metrics) recordCacheRollback() {
	atomic.AddInt64(&m.cacheRollbacks, 1)
}

func (m *metrics) recordCacheCompensation() {
	atomic.AddInt64(&m.cacheCompensations, 1)
}

func (m *metrics) recordQueueEnqueueFailure() {
	atomic.AddInt64(&m.queueEnqueueFailures, 1)
}

func (m *metrics) recordQueueEnqueueSuccess() {
	atomic.AddInt64(&m.queueEnqueueSuccess, 1)
}

// StatsSnapshot 运行时统计快照（非接口方法，按需断言使用）
type StatsSnapshot struct {
	Cache CacheStats
	Query TimingStats
	Write TimingStats
	Queue queue.Stats
	Batch map[string]interface{}
}

type CacheStats struct {
	L1Hits int64
	L2Hits int64
	DBHits int64
	Misses int64

	WriteFailures int64
	Rollbacks     int64
	Compensations int64

	QueueEnqueueFailures int64
	QueueEnqueueSuccess  int64
}

type TimingStats struct {
	Count int64
	Total time.Duration
	Avg   time.Duration
}

// Stats 返回统计快照（需要类型断言为 *zmsg 才能访问）
func (z *zmsg) Stats() StatsSnapshot {
	if z.metrics == nil {
		return StatsSnapshot{}
	}

	queryCount := atomic.LoadInt64(&z.metrics.queryCount)
	queryTotal := time.Duration(atomic.LoadInt64(&z.metrics.queryDurationNanos))
	writeCount := atomic.LoadInt64(&z.metrics.writeCount)
	writeTotal := time.Duration(atomic.LoadInt64(&z.metrics.writeDurationNanos))

	var queueStats queue.Stats
	if z.queue != nil {
		queueStats = z.queue.Stats()
	}
	var batchStats map[string]interface{}
	if z.periodicWriter != nil {
		batchStats = z.periodicWriter.Stats()
	}

	return StatsSnapshot{
		Cache: CacheStats{
			L1Hits:               atomic.LoadInt64(&z.metrics.l1Hits),
			L2Hits:               atomic.LoadInt64(&z.metrics.l2Hits),
			DBHits:               atomic.LoadInt64(&z.metrics.dbHits),
			Misses:               atomic.LoadInt64(&z.metrics.misses),
			WriteFailures:        atomic.LoadInt64(&z.metrics.cacheWriteFailures),
			Rollbacks:            atomic.LoadInt64(&z.metrics.cacheRollbacks),
			Compensations:        atomic.LoadInt64(&z.metrics.cacheCompensations),
			QueueEnqueueFailures: atomic.LoadInt64(&z.metrics.queueEnqueueFailures),
			QueueEnqueueSuccess:  atomic.LoadInt64(&z.metrics.queueEnqueueSuccess),
		},
		Query: TimingStats{
			Count: queryCount,
			Total: queryTotal,
			Avg:   avgDuration(queryCount, queryTotal),
		},
		Write: TimingStats{
			Count: writeCount,
			Total: writeTotal,
			Avg:   avgDuration(writeCount, writeTotal),
		},
		Queue: queueStats,
		Batch: batchStats,
	}
}

func avgDuration(count int64, total time.Duration) time.Duration {
	if count <= 0 {
		return 0
	}
	return time.Duration(int64(total) / count)
}
