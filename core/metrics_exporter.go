package zmsg

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsExporter Prometheus 指标导出器
type MetricsExporter struct {
	cacheHits    *prometheus.CounterVec
	cacheMisses  prometheus.Counter
	writeLatency prometheus.Histogram
	queryLatency prometheus.Histogram

	queueSize          prometheus.Gauge
	batchFlushTotal    *prometheus.CounterVec
	batchFlushDuration prometheus.Histogram
}

var (
	// globalExporter 全局导出器单例
	globalExporter *MetricsExporter
	exporterOnce   sync.Once

	// defaultBuckets 默认延迟分布桶 (秒)
	defaultBuckets = []float64{.0001, .00025, .0005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
)

// NewMetricsExporter 获取或创建全局单例导出器
func NewMetricsExporter(namespace string) *MetricsExporter {
	exporterOnce.Do(func() {
		if namespace == "" {
			namespace = "zmsg"
		}

		m := &MetricsExporter{
			cacheHits: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "cache_hits_total",
					Help:      "Total number of cache hits by level.",
				},
				[]string{"level"},
			),
			cacheMisses: prometheus.NewCounter(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "cache_misses_total",
					Help:      "Total number of cache misses (bloom filter passed but not found in L1/L2).",
				},
			),
			writeLatency: prometheus.NewHistogram(
				prometheus.HistogramOpts{
					Namespace: namespace,
					Name:      "write_latency_seconds",
					Help:      "Histogram of write operation latency.",
					Buckets:   defaultBuckets,
				},
			),
			queryLatency: prometheus.NewHistogram(
				prometheus.HistogramOpts{
					Namespace: namespace,
					Name:      "query_latency_seconds",
					Help:      "Histogram of query operation latency.",
					Buckets:   defaultBuckets,
				},
			),
			queueSize: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: namespace,
					Name:      "batch_writer_queue_size",
					Help:      "Current number of pending tasks in the batch writer queue.",
				},
			),
			batchFlushTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "batch_flush_total",
					Help:      "Total number of batch flush operations.",
				},
				[]string{"status"}, // success, failed
			),
			batchFlushDuration: prometheus.NewHistogram(
				prometheus.HistogramOpts{
					Namespace: namespace,
					Name:      "batch_flush_duration_seconds",
					Help:      "Histogram of batch flush duration.",
					Buckets:   defaultBuckets,
				},
			),
		}

		// 注册指标
		prometheus.MustRegister(m.cacheHits)
		prometheus.MustRegister(m.cacheMisses)
		prometheus.MustRegister(m.writeLatency)
		prometheus.MustRegister(m.queryLatency)
		prometheus.MustRegister(m.queueSize)
		prometheus.MustRegister(m.batchFlushTotal)
		prometheus.MustRegister(m.batchFlushDuration)

		globalExporter = m
	})

	return globalExporter
}

// Handler 返回 HTTP 处理器
func (m *MetricsExporter) Handler() http.Handler {
	return promhttp.Handler()
}
