package zmsg

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	redis "github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"golang.org/x/sync/singleflight"

	"github.com/tiz36/zmsg/internal/batch"
	"github.com/tiz36/zmsg/internal/cache"
	"github.com/tiz36/zmsg/internal/id"
	"github.com/tiz36/zmsg/internal/log"
	"github.com/tiz36/zmsg/internal/queue"
	sqlpkg "github.com/tiz36/zmsg/internal/sql"
)

// zmsg 实现 ZMsg 接口
type zmsg struct {
	// 存储层
	l1 *ristretto.Cache // L1 缓存
	l2 *redis.Client    // L2 缓存
	db *sql.DB          // PostgreSQL

	// 优化组件
	sf    *singleflight.Group
	bloom *cache.BloomFilter

	// 内部管理器
	idGen          id.Generator
	periodicWriter *batch.PeriodicWriter // 周期写入器
	store          *postgresStore
	sqlExec        *sqlpkg.Executor
	queue          *queue.Queue // 异步队列

	// 配置
	config Config

	// 状态
	mu      sync.RWMutex
	closed  bool
	logger  log.Logger
	metrics *metrics
}

// New 创建新的 zmsg 实例
func New(ctx context.Context, cfg Config) (ZMsg, error) {
	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	z := &zmsg{
		config: cfg,
		logger: log.NewWithConfig(log.Config{
			Level:             cfg.Log.Level,
			Encoding:          cfg.Log.Encoding,
			AddCaller:         cfg.Log.AddCaller,
			CallerSkip:        cfg.Log.CallerSkip,
			Development:       cfg.Log.Development,
			DisableStacktrace: cfg.Log.DisableStacktrace,
			StacktraceLevel:   cfg.Log.StacktraceLevel,
			OutputPaths:       cfg.Log.OutputPaths,
			ErrorOutputPaths:  cfg.Log.ErrorOutputPaths,
			TimeFormat:        cfg.Log.TimeFormat,
			ColorOutput:       cfg.Log.ColorOutput,
		}),
		metrics: newMetrics(),
	}

	// 初始化顺序很重要
	if err := z.initComponents(ctx); err != nil {
		return nil, err
	}

	// 启动后台协程
	go z.startBackgroundWorkers(ctx)

	z.logger.Info("zmsg.New returning")
	return z, nil
}

// initComponents 初始化所有组件
func (z *zmsg) initComponents(ctx context.Context) error {
	var err error

	// 1. 初始化 PostgreSQL
	z.db, err = sql.Open("postgres", z.config.Postgres.DSN)
	if err != nil {
		return wrapError("DB_INIT_FAILED", "failed to open database", err)
	}
	z.db.SetMaxOpenConns(z.config.Postgres.MaxOpenConns)
	z.db.SetMaxIdleConns(z.config.Postgres.MaxIdleConns)

	// 2. 初始化 Redis (L2)
	z.l2 = redis.NewClient(&redis.Options{
		Addr:     z.config.Redis.Addr,
		Password: z.config.Redis.Password,
		DB:       z.config.Redis.DB,
	})

	// 3. 初始化异步队列 (asynq)
	queueCfg := &queue.Config{
		RedisAddr:                        z.config.Queue.Addr,
		RedisPassword:                    z.config.Queue.Password,
		RedisDB:                          z.config.Queue.DB,
		Concurrency:                      z.config.Queue.Concurrency,
		Queues:                           z.config.Queue.Queues,
		RetryMax:                         z.config.Queue.RetryMax,
		RetryDelay:                       z.config.Queue.RetryDelay,
		TaskDelay:                        z.config.Queue.TaskDelay,
		FallbackToSyncStoreOnEnqueueFail: z.config.Queue.FallbackToSyncStoreOnEnqueueFail,
	}
	z.queue, err = queue.New(queueCfg)
	if err != nil {
		return wrapError("QUEUE_INIT_FAILED", "failed to create queue", err)
	}

	// 4. 初始化 L1 缓存
	z.l1, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: z.config.Cache.L1NumCounters,
		MaxCost:     z.config.Cache.L1MaxCost,
		BufferItems: DefaultL1BufferItems,
	})
	if err != nil {
		return wrapError("L1_INIT_FAILED", "failed to create L1 cache", err)
	}

	// 5. 初始化其他组件
	z.sf = &singleflight.Group{}

	// 初始化布隆过滤器
	bloomConfig := &cache.BloomConfig{
		Key:                  RedisPrefixBloom + "filter",
		Capacity:             uint(z.config.Cache.BloomCapacity),
		ErrorRate:            z.config.Cache.BloomErrorRate,
		SyncInterval:         z.config.Cache.BloomSyncInterval,
		EnableLocalCache:     z.config.Cache.BloomEnableLocalCache,
		LocalCacheMaxEntries: z.config.Cache.BloomLocalCacheMaxEntries,
		LocalCacheTTL:        z.config.Cache.BloomLocalCacheTTL,
		DeleteStrategy:       z.config.Cache.BloomDeleteStrategy,
		RedisTTL:             z.config.Cache.BloomRedisTTL,
	}
	z.bloom = cache.NewBloomFilter(bloomConfig, z.l2)

	// 初始化ID生成器
	idCfg := id.DefaultConfig()
	idCfg.NodeTTL = z.config.ID.NodeTTL
	idCfg.AutoNodeID = true
	storage, err := id.NewPostgresStorageFromDB(z.db)
	if err != nil {
		return wrapError("ID_STORAGE_INIT_FAILED", "failed to create id storage", err)
	}
	idCfg.Storage = storage
	z.idGen, err = id.NewSnowflake(idCfg)
	if err != nil {
		return wrapError("ID_GEN_INIT_FAILED", "failed to create id generator", err)
	}

	// 初始化周期写入器
	writerCfg := batch.WriterConfig{
		FlushInterval: z.config.Batch.Interval,
		MaxQueueSize:  z.config.Batch.MaxQueueSize,
		ShardCount:    z.config.Batch.WriterShards,
		FlushTimeout:  z.config.Batch.FlushTimeout,
		WALEnabled:    z.config.WAL.Enabled,
		WALDir:        z.config.WAL.Dir,
		WALNoSync:     z.config.WAL.NoSync,
	}
	z.periodicWriter = batch.NewPeriodicWriter(z.db, z.logger, writerCfg)

	// 初始化存储和执行器
	z.store = newPostgresStore(z.db)
	z.sqlExec = sqlpkg.NewExecutor(z.db, nil)
	z.logger.Info("zmsg initialized successfully")
	return nil
}

// startBackgroundWorkers 启动后台工作协程
func (z *zmsg) startBackgroundWorkers(ctx context.Context) {
	// 1. 启动周期写入器
	z.periodicWriter.Start()

	// 3. 启动队列 worker (asynq)
	go func() {
		if err := z.startQueueWorker(); err != nil {
			z.logger.Error("queue worker failed", "error", err)
		}
	}()

	// 4. 启动指标收集器
	if z.config.Log.MetricsEnabled {
		go z.collectMetrics(ctx)
	}
}

// ========== 核心接口实现 ==========

// CacheOnly 仅缓存
func (z *zmsg) CacheOnly(ctx context.Context, key string, value []byte, opts ...Option) error {
	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	options := buildOptions(opts...)
	ttl := options.TTL
	if ttl == 0 {
		ttl = z.config.DefaultTTL
	}

	// 更新 L1 和 L2 缓存
	if err := z.updateCache(ctx, key, value, ttl); err != nil {
		return wrapError("CACHE_FAILED", "cache only failed", err)
	}

	return nil
}

// CacheAndStore 缓存并立即存储
func (z *zmsg) CacheAndStore(ctx context.Context, key string, value []byte,
	sqlTask *SQLTask, opts ...Option) (string, error) {

	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()
	z.logger.Debug("[CacheAndStore] start", "key", key, "query", sqlTask.Query)

	// 1. 缓存数据
	options := buildOptions(opts...)
	ttl := options.TTL
	if ttl == 0 {
		ttl = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, ttl); err != nil {
		return "", wrapError("CACHE_FAILED", "cache update failed", err)
	}

	// 2. 执行 SQL 存储
	z.logger.Debug("[DB] exec start", "key", key, "query", sqlTask.Query, "params", sqlTask.Params)
	task := convertSQLTask(sqlTask)
	result, err := z.sqlExec.Execute(ctx, task)
	if err != nil {
		z.logger.Debug("[DB] exec failed", "key", key, "error", err)
		// 存储失败，清理缓存
		z.delCache(ctx, key)
		return "", wrapError("STORE_FAILED", "store failed", err)
	}
	z.logger.Debug("[DB] exec success", "key", key, "lastInsertID", result.LastInsertID, "rowsAffected", result.RowsAffected)

	// 3. 返回生成的 ID（如果有）
	if sqlTask.TaskType == TaskTypeContent && result.LastInsertID > 0 {
		return fmt.Sprintf("%s_%d", z.config.ID.Prefix, result.LastInsertID), nil
	}

	return key, nil
}

// CacheAndAsyncStore 缓存并 Asynq 延迟存储
// CacheAndDelayStore 缓存并延迟存储（asynq 延迟队列）
func (z *zmsg) CacheAndDelayStore(ctx context.Context, key string, value []byte,
	sqlTask *SQLTask, opts ...Option) error {

	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()
	z.logger.Debug("[CacheAndDelayStore] start", "key", key, "query", sqlTask.Query)

	// 1. 缓存数据
	options := buildOptions(opts...)
	ttl := options.TTL
	if ttl == 0 {
		ttl = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, ttl); err != nil {
		return wrapError("CACHE_FAILED", "cache update failed", err)
	}

	// 2. 加入 asynq 队列（支持延迟执行）
	payload := &queue.TaskPayload{
		Key:       key,
		Value:     value,
		Query:     sqlTask.Query,
		Params:    sqlTask.Params,
		CreatedAt: time.Now(),
	}

	// 使用 Option 中的 AsyncDelay，否则用配置的默认值
	delay := options.AsyncDelay
	if delay == 0 {
		delay = z.config.Queue.TaskDelay
	}

	var asynqOpts []asynq.Option
	if delay > 0 {
		asynqOpts = append(asynqOpts, asynq.ProcessIn(delay))
	}

	z.logger.Debug("[Asynq] enqueue task", "key", key, "delay", delay, "query", sqlTask.Query)
	if err := z.queue.EnqueueSave(ctx, payload, asynqOpts...); err != nil {
		z.logger.Debug("[Asynq] enqueue failed", "key", key, "error", err)
		z.metrics.recordQueueEnqueueFailure()
		if z.config.Queue.FallbackToSyncStoreOnEnqueueFail {
			z.logger.Warn("[Asynq] enqueue failed, fallback to sync store", "key", key)
			task := convertSQLTask(sqlTask)
			if _, execErr := z.sqlExec.Execute(ctx, task); execErr != nil {
				z.delCache(ctx, key)
				return wrapError("STORE_FAILED", "fallback store failed", execErr)
			}
			return nil
		}
		return wrapError("QUEUE_FAILED", "enqueue failed", err)
	}
	z.metrics.recordQueueEnqueueSuccess()
	z.logger.Debug("[Asynq] enqueue success", "key", key)

	return nil
}

// CacheAndPeriodicStore 缓存并周期存储（内存聚合 + 定时 flush）
// TaskType 决定聚合策略:
//   - TaskTypeContent: 覆盖（相同 BatchKey 只执行最新的）
//   - TaskTypeCounter: 内存累加聚合（多次 Inc/Dec 合并为一次 SQL）
//   - TaskTypeAppend: Slice 内存聚合（多次 Add/Del 合并为一次批量操作）
//   - TaskTypePut: Map 内存聚合（多次 Set/Del 合并为一次批量操作）
func (z *zmsg) CacheAndPeriodicStore(ctx context.Context, key string, value []byte,
	sqlTask *SQLTask, opts ...Option) error {

	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	taskTypeStr := []string{"Content", "Counter", "Slice", "Map"}[sqlTask.TaskType]
	z.logger.Debug("[CacheAndPeriodicStore] start",
		"key", key,
		"taskType", taskTypeStr,
		"batchKey", sqlTask.BatchKey,
		"table", sqlTask.Table,
		"column", sqlTask.Column)

	// 1. 缓存数据
	options := buildOptions(opts...)
	ttl := options.TTL
	if ttl == 0 {
		ttl = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, ttl); err != nil {
		return wrapError("CACHE_FAILED", "cache update failed", err)
	}

	// 2. 提交到周期写入器（传递聚合所需的字段）
	task := &batch.PeriodicTask{
		Key:      key,
		Value:    value,
		Query:    sqlTask.Query,
		Params:   sqlTask.Params,
		TaskType: batch.TaskType(sqlTask.TaskType),
		BatchKey: sqlTask.BatchKey,

		// 聚合信息
		Table:     sqlTask.Table,
		Column:    sqlTask.Column,
		Where:     sqlTask.Where,
		WhereArgs: sqlTask.WhereArgs,

		// Counter 聚合
		OpType: batch.OpType(sqlTask.OpType),
		Delta:  sqlTask.Delta,

		// Slice 聚合
		SliceValue: sqlTask.SliceValue,

		// Map 聚合
		MapKey:   sqlTask.MapKey,
		MapValue: sqlTask.MapValue,
	}

	z.logger.Debug("[Batch] submit task",
		"batchKey", task.BatchKey,
		"taskType", taskTypeStr,
		"delta", task.Delta,
		"sliceValue", task.SliceValue,
		"mapKey", task.MapKey)
	if err := z.periodicWriter.Submit(task); err != nil {
		z.logger.Debug("[Batch] submit failed", "batchKey", task.BatchKey, "error", err)
		if z.config.Queue.FallbackToSyncStoreOnEnqueueFail {
			z.logger.Warn("[Batch] submit failed, fallback to sync store", "batchKey", task.BatchKey)
			syncTask := convertSQLTask(sqlTask)
			if _, execErr := z.sqlExec.Execute(ctx, syncTask); execErr != nil {
				z.delCache(ctx, key)
				return wrapError("STORE_FAILED", "fallback store failed", execErr)
			}
			return nil
		}
		return wrapError("PERIODIC_WRITE_FAILED", "submit periodic task failed", err)
	}

	return nil
}

// Del 删除缓存
func (z *zmsg) Del(ctx context.Context, key string) error {
	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()
	return z.delCache(ctx, key)
}

// DelStore 删除并立即存储
func (z *zmsg) DelStore(ctx context.Context, key string, sqlTask *SQLTask) error {
	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	// 1. 删除缓存
	if err := z.delCache(ctx, key); err != nil {
		return wrapError("CACHE_FAILED", "delete cache failed", err)
	}

	// 2. 删除布隆过滤器记录
	z.bloom.Delete(ctx, key)

	// 3. 执行 SQL 删除
	task := convertSQLTask(sqlTask)
	_, err := z.sqlExec.Execute(ctx, task)
	if err != nil {
		return wrapError("STORE_FAILED", "delete store failed", err)
	}

	return nil
}

// DelDelayStore 删除并延迟存储
func (z *zmsg) DelDelayStore(ctx context.Context, key string, sqlTask *SQLTask) error {
	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	// 1. 删除缓存
	if err := z.delCache(ctx, key); err != nil {
		return wrapError("CACHE_FAILED", "delete cache failed", err)
	}

	// 2. 加入删除队列
	payload := &queue.TaskPayload{
		Key:       key,
		Query:     sqlTask.Query,
		Params:    sqlTask.Params,
		CreatedAt: time.Now(),
	}

	if err := z.queue.EnqueueDelete(ctx, payload); err != nil {
		z.metrics.recordQueueEnqueueFailure()
		if z.config.Queue.FallbackToSyncStoreOnEnqueueFail {
			z.logger.Warn("[Asynq] enqueue delete failed, fallback to sync store", "key", key)
			task := convertSQLTask(sqlTask)
			if _, execErr := z.sqlExec.Execute(ctx, task); execErr != nil {
				return wrapError("STORE_FAILED", "fallback delete failed", execErr)
			}
			return nil
		}
		return wrapError("QUEUE_FAILED", "enqueue failed", err)
	}
	z.metrics.recordQueueEnqueueSuccess()

	return nil
}

// Update 更新缓存
func (z *zmsg) Update(ctx context.Context, key string, value []byte) error {
	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	// 获取旧的 TTL
	oldTTL, err := z.getCacheTTL(ctx, key)
	if err != nil {
		oldTTL = z.config.DefaultTTL
	}

	// 更新缓存
	return z.updateCache(ctx, key, value, oldTTL)
}

// UpdateStore 更新并立即存储
func (z *zmsg) UpdateStore(ctx context.Context, key string, value []byte,
	sqlTask *SQLTask) error {

	z.checkClosed()
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordWriteLatency(time.Since(start))
		}
	}()

	// 1. 更新缓存
	oldTTL, err := z.getCacheTTL(ctx, key)
	if err != nil {
		oldTTL = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, oldTTL); err != nil {
		return wrapError("CACHE_FAILED", "update cache failed", err)
	}

	// 2. 执行 SQL 更新
	task := convertSQLTask(sqlTask)
	_, err = z.sqlExec.Execute(ctx, task)
	if err != nil {
		return wrapError("STORE_FAILED", "update store failed", err)
	}

	return nil
}

// SQLExec 执行 SQL
func (z *zmsg) SQLExec(ctx context.Context, sqlTask *SQLTask) (*SQLResult, error) {
	z.checkClosed()
	task := convertSQLTask(sqlTask)
	result, err := z.sqlExec.Execute(ctx, task)
	if err != nil {
		return nil, err
	}
	return &SQLResult{
		LastInsertID: result.LastInsertID,
		RowsAffected: result.RowsAffected,
	}, nil
}

// DBHit 检查 DB 命中（布隆过滤器）
func (z *zmsg) DBHit(ctx context.Context, key string) bool {
	z.checkClosed()
	return z.bloom.Test(ctx, key)
}

// NextID 生成下一个 ID
func (z *zmsg) NextID(ctx context.Context, prefix string) (string, error) {
	z.checkClosed()

	idStr, err := z.idGen.Generate(ctx)
	if err != nil {
		return "", wrapError("ID_GEN_FAILED", "generate id failed", err)
	}

	if prefix == "" {
		prefix = z.config.ID.Prefix
	}

	return fmt.Sprintf("%s_%s", prefix, idStr), nil
}

// Get 查询数据
func (z *zmsg) Get(ctx context.Context, key string) ([]byte, error) {
	z.checkClosed()

	// 查询管道：bloom -> L1 -> sf -> L2 -> DB -> 回填
	return z.queryPipeline(ctx, key)
}

// Batch 创建批处理操作
func (z *zmsg) Batch() BatchOperation {
	z.checkClosed()
	return newBatchOperation(z)
}

// Close 关闭 zmsg
func (z *zmsg) Close() error {
	z.mu.Lock()
	defer z.mu.Unlock()

	if z.closed {
		return nil
	}

	z.closed = true

	// 关闭所有组件
	var errs []error

	// 先停止周期写入器（确保 flush 所有数据）
	if z.periodicWriter != nil {
		z.periodicWriter.Stop()
	}

	if z.db != nil {
		if err := z.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if z.l2 != nil {
		if err := z.l2.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if z.bloom != nil {
		z.bloom.Close()
	}

	if z.queue != nil {
		if err := z.queue.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// 关闭批处理管理器（如果需要）
	if z.idGen != nil {
		z.idGen.Close()
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing zmsg: %v", errs)
	}

	return nil
}

// ========== 内部辅助方法 ==========

// checkClosed 检查是否已关闭
func (z *zmsg) checkClosed() {
	z.mu.RLock()
	defer z.mu.RUnlock()

	if z.closed {
		panic(ErrClosed)
	}
}

// updateCache 更新缓存（L1 + L2）
func (z *zmsg) updateCache(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return z.updateCacheWithPolicy(ctx, key, value, ttl, true)
}

func (z *zmsg) updateCacheWithPolicy(ctx context.Context, key string, value []byte, ttl time.Duration, allowCompensation bool) error {
	// 更新 L1
	z.l1.SetWithTTL(key, value, int64(len(value)), ttl)
	z.logger.Debug("[L1] cache set", "key", key, "size", len(value), "ttl", ttl)

	// 更新 L2 (Redis)
	if err := z.l2.Set(ctx, key, value, ttl).Err(); err != nil {
		z.logger.Debug("[L2/Redis] cache set failed", "key", key, "error", err)
		z.metrics.recordCacheWriteFailure()
		if z.config.Cache.RollbackL1OnL2Fail {
			z.l1.Del(key)
			z.metrics.recordCacheRollback()
		}
		if allowCompensation && z.config.Cache.EnqueueCompensationOnL2Fail {
			if err := z.enqueueCacheRepair(ctx, key, value, ttl); err != nil {
				z.logger.Error("[Cache] enqueue compensation failed", "key", key, "error", err)
				z.metrics.recordQueueEnqueueFailure()
			} else {
				z.metrics.recordCacheCompensation()
				z.metrics.recordQueueEnqueueSuccess()
			}
		}
		return err
	}
	z.logger.Debug("[L2/Redis] cache set", "key", key, "size", len(value), "ttl", ttl)

	// 更新布隆过滤器
	z.bloom.Add(ctx, key)
	z.logger.Debug("[Bloom] add key", "key", key)

	return nil
}

// delCache 删除缓存（L1 + L2）
func (z *zmsg) delCache(ctx context.Context, key string) error {
	// 删除 L1
	z.l1.Del(key)
	z.logger.Debug("[L1] cache del", "key", key)

	// 删除 L2 (Redis)
	if err := z.l2.Del(ctx, key).Err(); err != nil {
		z.logger.Debug("[L2/Redis] cache del failed", "key", key, "error", err)
		return err
	}
	z.logger.Debug("[L2/Redis] cache del", "key", key)

	return nil
}

func (z *zmsg) enqueueCacheRepair(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if z.queue == nil {
		return fmt.Errorf("queue not initialized")
	}
	payload := &queue.TaskPayload{
		Key:       key,
		Value:     value,
		TTL:       ttl,
		CreatedAt: time.Now(),
	}

	var opts []asynq.Option
	if z.config.Cache.CompensationDelay > 0 {
		opts = append(opts, asynq.ProcessIn(z.config.Cache.CompensationDelay))
	}
	return z.queue.EnqueueCacheRepair(ctx, payload, opts...)
}

// getCacheTTL 获取缓存的剩余 TTL
func (z *zmsg) getCacheTTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := z.l2.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return ttl, nil
}

// queryPipeline 查询管道
func (z *zmsg) queryPipeline(ctx context.Context, key string) ([]byte, error) {
	z.logger.Debug("[Query] pipeline start", "key", key)
	start := time.Now()
	defer func() {
		if z.metrics != nil {
			z.metrics.recordQueryLatency(time.Since(start))
		}
	}()

	// 1. 布隆过滤器检查
	if !z.bloom.Test(ctx, key) {
		z.logger.Debug("[Bloom] key not exists (blocked)", "key", key)
		if z.metrics != nil {
			z.metrics.recordCacheMiss()
		}
		return nil, ErrNotFound
	}
	z.logger.Debug("[Bloom] key may exist (pass)", "key", key)

	// 2. L1 缓存检查
	if value, found := z.l1.Get(key); found {
		z.logger.Debug("[L1] cache HIT", "key", key, "size", len(value.([]byte)))
		z.recordCacheHit("l1")
		return value.([]byte), nil
	}
	z.logger.Debug("[L1] cache MISS", "key", key)

	// 3. SingleFlight 防止缓存击穿
	value, err, _ := z.sf.Do(key, func() (interface{}, error) {
		// 4. L2 缓存检查
		value, err := z.l2.Get(ctx, key).Bytes()
		if err == nil {
			z.logger.Debug("[L2/Redis] cache HIT", "key", key, "size", len(value))
			// 回填 L1 缓存
			z.l1.Set(key, value, int64(len(value)))
			z.logger.Debug("[L1] backfill from L2", "key", key)
			z.recordCacheHit("l2")
			return value, nil
		}
		z.logger.Debug("[L2/Redis] cache MISS", "key", key)

		// 5. 数据库查询
		z.logger.Debug("[DB] query start", "key", key)
		data, err := z.store.Get(ctx, key)
		if err != nil {
			if err == sql.ErrNoRows {
				z.logger.Debug("[DB] not found, bloom false positive", "key", key)
				// 布隆过滤器误判，移除
				z.bloom.Delete(ctx, key)
				if z.metrics != nil {
					z.metrics.recordCacheMiss()
				}
				return nil, ErrNotFound
			}
			z.logger.Debug("[DB] query error", "key", key, "error", err)
			return nil, err
		}
		z.logger.Debug("[DB] query success", "key", key, "size", len(data))

		// 6. 回填缓存
		z.updateCache(ctx, key, data, z.config.DefaultTTL)
		z.logger.Debug("[Cache] backfill from DB", "key", key)
		z.recordCacheHit("db")

		return data, nil
	})

	if err != nil {
		return nil, err
	}

	return value.([]byte), nil
}

// registerQueueHandlers 注册队列处理器
func (z *zmsg) registerQueueHandlers() {
	// 保存任务处理器
	z.queue.RegisterHandler(queue.TypeSave, func(ctx context.Context, payload *queue.TaskPayload) error {
		z.logger.Debug("[Asynq] process SAVE task",
			"key", payload.Key,
			"query", payload.Query,
			"createdAt", payload.CreatedAt)
		task := sqlpkg.NewTask(payload.Query, payload.Params...)
		result, err := z.sqlExec.Execute(ctx, task)
		if err != nil {
			z.logger.Error("[Asynq] SAVE task failed", "key", payload.Key, "error", err)
			return err
		}
		z.logger.Debug("[Asynq] SAVE task success",
			"key", payload.Key,
			"rowsAffected", result.RowsAffected)
		return nil
	})

	// 删除任务处理器
	z.queue.RegisterHandler(queue.TypeDelete, func(ctx context.Context, payload *queue.TaskPayload) error {
		z.logger.Debug("[Asynq] process DELETE task",
			"key", payload.Key,
			"query", payload.Query)
		task := sqlpkg.NewTask(payload.Query, payload.Params...)
		result, err := z.sqlExec.Execute(ctx, task)
		if err != nil {
			z.logger.Error("[Asynq] DELETE task failed", "key", payload.Key, "error", err)
			return err
		}
		z.logger.Debug("[Asynq] DELETE task success",
			"key", payload.Key,
			"rowsAffected", result.RowsAffected)
		return nil
	})

	// 更新任务处理器
	z.queue.RegisterHandler(queue.TypeUpdate, func(ctx context.Context, payload *queue.TaskPayload) error {
		z.logger.Debug("[Asynq] process UPDATE task",
			"key", payload.Key,
			"query", payload.Query)
		task := sqlpkg.NewTask(payload.Query, payload.Params...)
		result, err := z.sqlExec.Execute(ctx, task)
		if err != nil {
			z.logger.Error("[Asynq] UPDATE task failed", "key", payload.Key, "error", err)
			return err
		}
		z.logger.Debug("[Asynq] UPDATE task success",
			"key", payload.Key,
			"rowsAffected", result.RowsAffected)
		return err
	})

	// 缓存修复任务处理器
	z.queue.RegisterHandler(queue.TypeCacheRepair, func(ctx context.Context, payload *queue.TaskPayload) error {
		ttl := payload.TTL
		if ttl == 0 {
			ttl = z.config.DefaultTTL
		}
		z.logger.Debug("[Asynq] process CACHE_REPAIR task",
			"key", payload.Key,
			"ttl", ttl)
		if err := z.updateCacheWithPolicy(ctx, payload.Key, payload.Value, ttl, false); err != nil {
			z.logger.Error("[Asynq] CACHE_REPAIR task failed", "key", payload.Key, "error", err)
			return err
		}
		z.logger.Debug("[Asynq] CACHE_REPAIR task success", "key", payload.Key)
		return nil
	})
}

// startQueueWorker 启动队列 worker
func (z *zmsg) startQueueWorker() error {
	z.registerQueueHandlers()
	return z.queue.Start()
}

// recordCacheHit 记录缓存命中
func (z *zmsg) recordCacheHit(level string) {
	// 这里可以添加监控指标
	if z.metrics != nil {
		z.metrics.recordCacheHit(level)
	}
	z.logger.Debug("cache hit", "level", level)
}

// convertSQLTask 转换 SQLTask 到 sqlpkg.Task
func convertSQLTask(task *SQLTask) *sqlpkg.Task {
	if task == nil {
		return nil
	}
	return sqlpkg.NewTask(task.Query, task.Params...)
}

// postgresStore PostgreSQL 存储
type postgresStore struct {
	db *sql.DB
}

func newPostgresStore(db *sql.DB) *postgresStore {
	return &postgresStore{db: db}
}

func (s *postgresStore) Get(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, "SELECT data FROM zmsg_data WHERE id = $1", key).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// collectMetrics 收集指标
func (z *zmsg) collectMetrics(ctx context.Context) {
	// 指标收集实现
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 收集指标
			z.logger.Debug("collecting metrics")
		}
	}
}

// batchOperation 批处理操作实现
type batchOperation struct {
	z     *zmsg
	items []batchItem
	mu    sync.Mutex
}

type batchItem struct {
	op      string
	key     string
	value   []byte
	sqlTask *SQLTask
	opts    []Option
}

func newBatchOperation(z *zmsg) BatchOperation {
	return &batchOperation{
		z:     z,
		items: make([]batchItem, 0),
	}
}

func (b *batchOperation) CacheOnly(key string, value []byte, opts ...Option) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, batchItem{
		op:    "cache_only",
		key:   key,
		value: value,
		opts:  opts,
	})
}

func (b *batchOperation) CacheAndStore(key string, value []byte, sqlTask *SQLTask, opts ...Option) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, batchItem{
		op:      "cache_and_store",
		key:     key,
		value:   value,
		sqlTask: sqlTask,
		opts:    opts,
	})
}

func (b *batchOperation) CacheAndDelayStore(key string, value []byte, sqlTask *SQLTask, opts ...Option) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, batchItem{
		op:      "cache_and_delay_store",
		key:     key,
		value:   value,
		sqlTask: sqlTask,
		opts:    opts,
	})
}

func (b *batchOperation) Execute(ctx context.Context) error {
	b.mu.Lock()
	items := make([]batchItem, len(b.items))
	copy(items, b.items)
	b.mu.Unlock()

	for _, item := range items {
		switch item.op {
		case "cache_only":
			if err := b.z.CacheOnly(ctx, item.key, item.value, item.opts...); err != nil {
				return err
			}
		case "cache_and_store":
			if _, err := b.z.CacheAndStore(ctx, item.key, item.value, item.sqlTask, item.opts...); err != nil {
				return err
			}
		case "cache_and_delay_store":
			if err := b.z.CacheAndDelayStore(ctx, item.key, item.value, item.sqlTask, item.opts...); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *batchOperation) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = b.items[:0]
}

func (b *batchOperation) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// MetricsHandler 实现 ZMsg 接口
func (z *zmsg) MetricsHandler() http.Handler {
	if z.metrics != nil && z.metrics.Exporter() != nil {
		return z.metrics.Exporter().Handler()
	}
	return http.NotFoundHandler()
}
