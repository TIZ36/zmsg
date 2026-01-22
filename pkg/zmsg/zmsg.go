package zmsg

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-redis/redis/v8"
)

// zmsg 实现 ZMsg 接口
type zmsg struct {
	// 存储层
	l1 *ristretto.Cache // L1 缓存
	l2 *redis.Client    // L2 缓存
	q  *redis.Client    // 队列 Redis
	db *sql.DB          // PostgreSQL

	// 优化组件
	sf    *singleflight.Group
	bloom *bloomFilter

	// 内部管理器
	idGen    *snowflakeGenerator
	batchMgr *batchManager
	store    *postgresStore
	sqlExec  *sqlExecutor

	// 配置
	config  Config
	options Options

	// 状态
	mu     sync.RWMutex
	closed bool
	logger Logger
}

// New 创建新的 zmsg 实例
func New(ctx context.Context, cfg Config) (ZMsg, error) {
	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	z := &zmsg{
		config: cfg,
		logger: newLogger(cfg.LogLevel),
	}

	// 初始化顺序很重要
	if err := z.initComponents(ctx); err != nil {
		return nil, err
	}

	// 启动后台协程
	go z.startBackgroundWorkers(ctx)

	return z, nil
}

// initComponents 初始化所有组件
func (z *zmsg) initComponents(ctx context.Context) error {
	var err error

	// 1. 初始化 PostgreSQL
	z.db, err = sql.Open("postgres", z.config.PostgresDSN)
	if err != nil {
		return wrapError("DB_INIT_FAILED", "failed to open database", err)
	}
	z.db.SetMaxOpenConns(z.config.MaxOpenConns)
	z.db.SetMaxIdleConns(z.config.MaxIdleConns)

	// 2. 初始化 Redis (L2)
	z.l2 = redis.NewClient(&redis.Options{
		Addr:     z.config.RedisAddr,
		Password: z.config.RedisPassword,
		DB:       z.config.RedisDB,
	})

	// 3. 初始化队列 Redis
	z.q = redis.NewClient(&redis.Options{
		Addr:     z.config.QueueAddr,
		Password: z.config.QueuePassword,
		DB:       z.config.QueueDB,
	})

	// 4. 初始化 L1 缓存
	z.l1, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: z.config.L1NumCounters,
		MaxCost:     z.config.L1MaxCost,
		BufferItems: DefaultL1BufferItems,
	})
	if err != nil {
		return wrapError("L1_INIT_FAILED", "failed to create L1 cache", err)
	}

	// 5. 初始化其他组件
	z.sf = &singleflight.Group{}
	z.bloom = newBloomFilter(z.l2, z.config.BloomCapacity, z.config.BloomErrorRate)
	z.idGen = newSnowflakeGenerator(z.db, z.l2, z.config.NodeTTL)
	z.batchMgr = newBatchManager(z.config.BatchSize, z.config.BatchInterval, z.config.FlushThreshold)
	z.store = newPostgresStore(z.db)
	z.sqlExec = newSQLExecutor(z.db)

	// 6. 运行数据库迁移
	if err := z.runMigrations(ctx); err != nil {
		return wrapError("MIGRATION_FAILED", "failed to run migrations", err)
	}

	z.logger.Info("zmsg initialized successfully")
	return nil
}

// runMigrations 运行数据库迁移
func (z *zmsg) runMigrations(ctx context.Context) error {
	migrations := []string{
		// 创建节点表
		`CREATE TABLE IF NOT EXISTS zmsg_nodes (
            id SERIAL PRIMARY KEY,
            node_id INTEGER NOT NULL,
            hostname VARCHAR(255),
            ip VARCHAR(50),
            service_name VARCHAR(255),
            last_seen TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '1 minute',
            UNIQUE(node_id)
        )`,

		// 创建数据表
		`CREATE TABLE IF NOT EXISTS zmsg_data (
            id VARCHAR(255) PRIMARY KEY,
            data BYTEA,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP
        )`,

		// 创建计数器表
		`CREATE TABLE IF NOT EXISTS zmsg_counters (
            key VARCHAR(255) PRIMARY KEY,
            value BIGINT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )`,
	}

	for _, migration := range migrations {
		if _, err := z.db.ExecContext(ctx, migration); err != nil {
			return err
		}
	}

	return nil
}

// startBackgroundWorkers 启动后台工作协程
func (z *zmsg) startBackgroundWorkers(ctx context.Context) {
	// 1. 启动批处理刷新器
	go z.batchMgr.flushLoop(ctx, z.store)

	// 2. 启动队列消费者
	go z.startQueueConsumer(ctx)

	// 3. 启动指标收集器
	if z.config.MetricsEnabled {
		go z.collectMetrics(ctx)
	}

	// 4. 启动心跳协程
	go z.idGen.heartbeat(ctx)
}

// ========== 核心接口实现 ==========

// CacheOnly 仅缓存
func (z *zmsg) CacheOnly(ctx context.Context, key string, value []byte, opts ...Option) error {
	z.checkClosed()

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
	result, err := z.sqlExec.Execute(ctx, sqlTask)
	if err != nil {
		// 存储失败，清理缓存
		z.delCache(ctx, key)
		return "", wrapError("STORE_FAILED", "store failed", err)
	}

	// 3. 返回生成的 ID（如果有）
	if sqlTask.TaskType == TaskTypeContent && result.LastInsertID > 0 {
		return fmt.Sprintf("%s_%d", z.config.IDPrefix, result.LastInsertID), nil
	}

	return key, nil
}

// CacheAndDelayStore 缓存并延迟存储
func (z *zmsg) CacheAndDelayStore(ctx context.Context, key string, value []byte,
	sqlTask *SQLTask, opts ...Option) error {

	z.checkClosed()

	// 1. 缓存数据
	options := buildOptions(opts...)
	ttl := options.TTL
	if ttl == 0 {
		ttl = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, ttl); err != nil {
		return wrapError("CACHE_FAILED", "cache update failed", err)
	}

	// 2. 加入异步队列
	queueTask := &QueueTask{
		Type:      TaskTypeSave,
		Key:       key,
		Value:     value,
		SQLTask:   sqlTask,
		Options:   options,
		CreatedAt: time.Now(),
	}

	if err := z.enqueueTask(ctx, queueTask); err != nil {
		return wrapError("QUEUE_FAILED", "enqueue failed", err)
	}

	return nil
}

// Del 删除缓存
func (z *zmsg) Del(ctx context.Context, key string) error {
	z.checkClosed()
	return z.delCache(ctx, key)
}

// DelStore 删除并立即存储
func (z *zmsg) DelStore(ctx context.Context, key string, sqlTask *SQLTask) error {
	z.checkClosed()

	// 1. 删除缓存
	if err := z.delCache(ctx, key); err != nil {
		return wrapError("CACHE_FAILED", "delete cache failed", err)
	}

	// 2. 删除布隆过滤器记录
	z.bloom.Delete(ctx, key)

	// 3. 执行 SQL 删除
	_, err := z.sqlExec.Execute(ctx, sqlTask)
	if err != nil {
		return wrapError("STORE_FAILED", "delete store failed", err)
	}

	return nil
}

// DelDelayStore 删除并延迟存储
func (z *zmsg) DelDelayStore(ctx context.Context, key string, sqlTask *SQLTask) error {
	z.checkClosed()

	// 1. 删除缓存
	if err := z.delCache(ctx, key); err != nil {
		return wrapError("CACHE_FAILED", "delete cache failed", err)
	}

	// 2. 加入删除队列
	queueTask := &QueueTask{
		Type:      TaskTypeDelete,
		Key:       key,
		SQLTask:   sqlTask,
		CreatedAt: time.Now(),
	}

	if err := z.enqueueTask(ctx, queueTask); err != nil {
		return wrapError("QUEUE_FAILED", "enqueue failed", err)
	}

	return nil
}

// Update 更新缓存
func (z *zmsg) Update(ctx context.Context, key string, value []byte) error {
	z.checkClosed()

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

	// 1. 更新缓存
	oldTTL, err := z.getCacheTTL(ctx, key)
	if err != nil {
		oldTTL = z.config.DefaultTTL
	}

	if err := z.updateCache(ctx, key, value, oldTTL); err != nil {
		return wrapError("CACHE_FAILED", "update cache failed", err)
	}

	// 2. 执行 SQL 更新
	_, err = z.sqlExec.Execute(ctx, sqlTask)
	if err != nil {
		return wrapError("STORE_FAILED", "update store failed", err)
	}

	return nil
}

// SQLExec 执行 SQL
func (z *zmsg) SQLExec(ctx context.Context, sqlTask *SQLTask) (*SQLResult, error) {
	z.checkClosed()
	return z.sqlExec.Execute(ctx, sqlTask)
}

// DBHit 检查 DB 命中（布隆过滤器）
func (z *zmsg) DBHit(ctx context.Context, key string) bool {
	z.checkClosed()
	return z.bloom.Test(ctx, key)
}

// NextID 生成下一个 ID
func (z *zmsg) NextID(ctx context.Context, prefix string) (string, error) {
	z.checkClosed()

	id, err := z.idGen.Generate()
	if err != nil {
		return "", wrapError("ID_GEN_FAILED", "generate id failed", err)
	}

	if prefix == "" {
		prefix = z.config.IDPrefix
	}

	return fmt.Sprintf("%s_%d", prefix, id), nil
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

	if z.q != nil {
		if err := z.q.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	z.batchMgr.stop()

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
	// 更新 L1
	z.l1.SetWithTTL(key, value, int64(len(value)), ttl)

	// 更新 L2
	if err := z.l2.Set(ctx, key, value, ttl).Err(); err != nil {
		return err
	}

	// 更新布隆过滤器
	z.bloom.Add(ctx, key)

	return nil
}

// delCache 删除缓存（L1 + L2）
func (z *zmsg) delCache(ctx context.Context, key string) error {
	// 删除 L1
	z.l1.Del(key)

	// 删除 L2
	if err := z.l2.Del(ctx, key).Err(); err != nil {
		return err
	}

	return nil
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
	// 1. 布隆过滤器检查
	if !z.bloom.Test(ctx, key) {
		return nil, ErrNotFound
	}

	// 2. L1 缓存检查
	if value, found := z.l1.Get(key); found {
		z.recordCacheHit("l1")
		return value.([]byte), nil
	}

	// 3. SingleFlight 防止缓存击穿
	value, err, _ := z.sf.Do(key, func() (interface{}, error) {
		// 4. L2 缓存检查
		value, err := z.l2.Get(ctx, key).Bytes()
		if err == nil {
			// 回填 L1 缓存
			z.l1.Set(key, value, int64(len(value)))
			z.recordCacheHit("l2")
			return value, nil
		}

		// 5. 数据库查询
		data, err := z.store.Get(ctx, key)
		if err != nil {
			if err == sql.ErrNoRows {
				// 布隆过滤器误判，移除
				z.bloom.Delete(ctx, key)
				return nil, ErrNotFound
			}
			return nil, err
		}

		// 6. 回填缓存
		z.updateCache(ctx, key, data, z.config.DefaultTTL)
		z.recordCacheHit("db")

		return data, nil
	})

	if err != nil {
		return nil, err
	}

	return value.([]byte), nil
}

// enqueueTask 加入队列
func (z *zmsg) enqueueTask(ctx context.Context, task *QueueTask) error {
	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	// 根据优先级选择队列
	queueKey := fmt.Sprintf("%s%d", RedisPrefixQueue, task.Options.Priority)

	return z.q.LPush(ctx, queueKey, taskBytes).Err()
}

// startQueueConsumer 启动队列消费者
func (z *zmsg) startQueueConsumer(ctx context.Context) {
	for priority := 0; priority <= 10; priority++ {
		go z.consumeQueue(ctx, priority)
	}
}

// consumeQueue 消费队列
func (z *zmsg) consumeQueue(ctx context.Context, priority int) {
	queueKey := fmt.Sprintf("%s%d", RedisPrefixQueue, priority)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// BRPop 阻塞获取任务
			result, err := z.q.BRPop(ctx, 0, queueKey).Result()
			if err != nil {
				z.logger.Error("queue pop failed", "error", err)
				time.Sleep(time.Second)
				continue
			}

			if len(result) < 2 {
				continue
			}

			taskBytes := result[1]
			var task QueueTask
			if err := json.Unmarshal([]byte(taskBytes), &task); err != nil {
				z.logger.Error("unmarshal task failed", "error", err)
				continue
			}

			// 处理任务
			z.processQueueTask(ctx, &task)
		}
	}
}

// processQueueTask 处理队列任务
func (z *zmsg) processQueueTask(ctx context.Context, task *QueueTask) {
	var err error

	switch task.Type {
	case TaskTypeSave:
		_, err = z.sqlExec.Execute(ctx, task.SQLTask)
	case TaskTypeDelete:
		_, err = z.sqlExec.Execute(ctx, task.SQLTask)
	case TaskTypeUpdate:
		_, err = z.sqlExec.Execute(ctx, task.SQLTask)
	}

	if err != nil {
		z.logger.Error("process queue task failed",
			"type", task.Type,
			"key", task.Key,
			"error", err)

		// 重试逻辑
		if task.RetryCount < QueueMaxRetries {
			task.RetryCount++
			time.Sleep(QueueRetryDelay * time.Duration(task.RetryCount))
			z.enqueueTask(ctx, task)
		}
	}
}

// recordCacheHit 记录缓存命中
func (z *zmsg) recordCacheHit(level string) {
	// 这里可以添加监控指标
	z.logger.Debug("cache hit", "level", level)
}
