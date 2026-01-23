package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
)

// 任务类型常量
const (
	TypeSave   = "zmsg:save"
	TypeDelete = "zmsg:delete"
	TypeUpdate = "zmsg:update"
	TypeCacheRepair = "zmsg:cache_repair"
)

// Config 队列配置
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Concurrency   int           // 并发 worker 数
	Queues        map[string]int // 队列权重
	RetryMax      int           // 最大重试次数
	RetryDelay    time.Duration // 重试延迟
	TaskDelay     time.Duration // 任务延迟执行时间
	FallbackToSyncStoreOnEnqueueFail bool
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		RedisAddr:   "localhost:6379",
		Concurrency: 10,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		RetryMax:    3,
		RetryDelay:  time.Second * 5,
	}
}

// Queue 基于 asynq 的异步队列
type Queue struct {
	client *asynq.Client
	server *asynq.Server
	mux    *asynq.ServeMux
	config *Config
	redisOpt asynq.RedisClientOpt
	stats *Stats
	mu    sync.Mutex
}

// New 创建队列
func New(cfg *Config) (*Queue, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if cfg.Queues == nil || len(cfg.Queues) == 0 {
		cfg.Queues = DefaultConfig().Queues
	}

	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}

	client := asynq.NewClient(redisOpt)

	server := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues:      cfg.Queues,
		RetryDelayFunc: func(n int, e error, t *asynq.Task) time.Duration {
			return cfg.RetryDelay * time.Duration(n)
		},
	})

	return &Queue{
		client: client,
		server: server,
		mux:    asynq.NewServeMux(),
		config: cfg,
		redisOpt: redisOpt,
		stats:  &Stats{},
	}, nil
}

// TaskPayload 任务载荷
type TaskPayload struct {
	Key       string                 `json:"key"`
	Value     []byte                 `json:"value,omitempty"`
	TTL       time.Duration          `json:"ttl,omitempty"`
	Query     string                 `json:"query"`
	Params    []interface{}          `json:"params,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
}

// EnqueueSave 入队保存任务
func (q *Queue) EnqueueSave(ctx context.Context, payload *TaskPayload, opts ...asynq.Option) error {
	return q.enqueue(ctx, TypeSave, payload, opts...)
}

// EnqueueDelete 入队删除任务
func (q *Queue) EnqueueDelete(ctx context.Context, payload *TaskPayload, opts ...asynq.Option) error {
	return q.enqueue(ctx, TypeDelete, payload, opts...)
}

// EnqueueUpdate 入队更新任务
func (q *Queue) EnqueueUpdate(ctx context.Context, payload *TaskPayload, opts ...asynq.Option) error {
	return q.enqueue(ctx, TypeUpdate, payload, opts...)
}

// EnqueueCacheRepair 入队缓存修复任务
func (q *Queue) EnqueueCacheRepair(ctx context.Context, payload *TaskPayload, opts ...asynq.Option) error {
	return q.enqueue(ctx, TypeCacheRepair, payload, opts...)
}

func (q *Queue) enqueue(ctx context.Context, taskType string, payload *TaskPayload, opts ...asynq.Option) error {
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = time.Now()
	}

	atomic.AddInt64(&q.stats.EnqueueTotal, 1)
	data, err := json.Marshal(payload)
	if err != nil {
		atomic.AddInt64(&q.stats.EnqueueFailed, 1)
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	task := asynq.NewTask(taskType, data, opts...)
	_, err = q.client.EnqueueContext(ctx, task)
	if err != nil {
		atomic.AddInt64(&q.stats.EnqueueFailed, 1)
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	return nil
}

// Handler 任务处理器类型
type Handler func(ctx context.Context, payload *TaskPayload) error

// RegisterHandler 注册任务处理器
func (q *Queue) RegisterHandler(taskType string, handler Handler) {
	q.mux.HandleFunc(taskType, func(ctx context.Context, t *asynq.Task) error {
		var payload TaskPayload
		if err := json.Unmarshal(t.Payload(), &payload); err != nil {
			atomic.AddInt64(&q.stats.ProcessFailed, 1)
			return fmt.Errorf("failed to unmarshal payload: %w", err)
		}
		err := handler(ctx, &payload)
		atomic.AddInt64(&q.stats.Processed, 1)
		if err != nil {
			atomic.AddInt64(&q.stats.ProcessFailed, 1)
		}
		return err
	})
}

// Start 启动 worker
func (q *Queue) Start() error {
	return q.server.Start(q.mux)
}

// Stop 停止 worker
func (q *Queue) Stop() {
	q.server.Stop()
}

// Shutdown 优雅关闭
func (q *Queue) Shutdown() {
	q.server.Shutdown()
}

// Close 关闭队列
func (q *Queue) Close() error {
	q.server.Shutdown()
	return q.client.Close()
}

// GetClient 获取 asynq 客户端（用于高级操作）
func (q *Queue) GetClient() *asynq.Client {
	return q.client
}

// UpdateConfig 动态更新队列配置（可选）
func (q *Queue) UpdateConfig(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	if cfg.Queues == nil || len(cfg.Queues) == 0 {
		cfg.Queues = q.config.Queues
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.config = cfg
	q.server.Shutdown()
	q.server = asynq.NewServer(q.redisOpt, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues:      cfg.Queues,
		RetryDelayFunc: func(n int, e error, t *asynq.Task) time.Duration {
			return cfg.RetryDelay * time.Duration(n)
		},
	})
	return nil
}

// Stats 队列统计信息
func (q *Queue) Stats() Stats {
	return Stats{
		EnqueueTotal:  atomic.LoadInt64(&q.stats.EnqueueTotal),
		EnqueueFailed: atomic.LoadInt64(&q.stats.EnqueueFailed),
		Processed:     atomic.LoadInt64(&q.stats.Processed),
		ProcessFailed: atomic.LoadInt64(&q.stats.ProcessFailed),
	}
}

type Stats struct {
	EnqueueTotal  int64
	EnqueueFailed int64
	Processed     int64
	ProcessFailed int64
}
