package batch

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/wal"
)

// TaskType 任务类型
type TaskType int

const (
	TaskTypeContent TaskType = iota // 覆盖（只执行最新）
	TaskTypeCounter                 // 计数器累加
	TaskTypeMerge                   // 合并写入 (INSERT ON CONFLICT DO UPDATE SET col = col || excluded.col)
)

// OpType 操作类型
type OpType int

const (
	OpInc   OpType = iota // 累加
	OpDec                 // 递减
	OpMul                 // 乘法
	OpSet                 // 设置
	OpClean               // 清零/清空
	OpAdd                 // 数组追加
	OpDel                 // 删除
)

// PeriodicTask 周期任务
type PeriodicTask struct {
	Key      string
	Value    []byte
	Query    string
	Params   []interface{}
	TaskType TaskType
	BatchKey string // 用于聚合的 key（通常是 table:where_condition）

	// 聚合信息
	Table     string        // 表名
	Column    string        // 列名
	Where     string        // WHERE 条件（不含 WHERE 关键字）
	WhereArgs []interface{} // WHERE 参数

	// Counter 聚合
	OpType OpType      // 操作类型
	Delta  interface{} // 增量值（int64, float64）
}

// PeriodicWriter 周期写入器
type PeriodicWriter struct {
	db     *sql.DB
	logger Logger

	// 任务缓冲（按 BatchKey 分组，分片降低锁竞争）
	shards     []taskShard
	shardCount int

	// 任务计数（用于长度触发）
	taskCount int64

	// 配置
	flushInterval time.Duration
	maxQueueSize  int64 // 队列长度阈值，达到后立即 flush
	flushTimeout  time.Duration

	// 控制
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	flushCh  chan struct{} // flush 信号通道
	flushing int32         // 是否正在 flush（原子操作）

	// WAL
	walEnabled   bool
	walLog       *wal.Log
	lastWalIndex uint64
	walMu        sync.Mutex

	stats writerStats
}

// TaskGroup 任务组（按 BatchKey 聚合）
type TaskGroup struct {
	BatchKey  string
	TaskType  TaskType
	Table     string
	Column    string
	Where     string
	WhereArgs []interface{}

	// Content 类型：只保留最新的任务
	LatestTask *PeriodicTask

	// Counter 聚合状态
	CounterAgg *CounterAggState

	// Merge 聚合状态
	MergeAgg *MergeAggState

	mu sync.Mutex
}

type taskShard struct {
	mu    sync.RWMutex
	tasks map[string]*TaskGroup
}

type writerStats struct {
	flushTotal        int64
	lastFlushUnixNano int64
}

// CounterAggState 计数器聚合状态
type CounterAggState struct {
	Delta     float64 // 累加的增量
	OpType    OpType  // 最终操作类型
	HasMul    bool    // 是否有乘法操作
	MulFactor float64 // 乘法因子
}

// MergeAggState Merge 聚合状态
type MergeAggState struct {
	Data map[string]interface{}
}

// Logger 日志接口
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

// WriterConfig 配置
type WriterConfig struct {
	FlushInterval time.Duration // 周期 flush 间隔
	MaxQueueSize  int64         // 队列长度阈值
	ShardCount    int           // 分片数量
	FlushTimeout  time.Duration // flush 超时

	// WAL 配置
	WALEnabled bool
	WALDir     string
	WALNoSync  bool
}

// NewPeriodicWriter 创建周期写入器
func NewPeriodicWriter(db *sql.DB, logger Logger, cfg WriterConfig) *PeriodicWriter {
	ctx, cancel := context.WithCancel(context.Background())

	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 16
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 30 * time.Second
	}

	w := &PeriodicWriter{
		db:            db,
		logger:        logger,
		flushInterval: cfg.FlushInterval,
		maxQueueSize:  cfg.MaxQueueSize,
		flushTimeout:  cfg.FlushTimeout,
		shardCount:    cfg.ShardCount,
		ctx:           ctx,
		cancel:        cancel,
		flushCh:       make(chan struct{}, 1),
		walEnabled:    cfg.WALEnabled,
	}

	if w.walEnabled {
		if err := os.MkdirAll(cfg.WALDir, 0755); err != nil {
			logger.Error("failed to create WAL directory", "error", err, "dir", cfg.WALDir)
			w.walEnabled = false
		} else {
			opts := wal.DefaultOptions
			opts.NoSync = cfg.WALNoSync
			log, err := wal.Open(cfg.WALDir, opts)
			if err != nil {
				logger.Error("failed to open WAL", "error", err, "dir", cfg.WALDir)
				w.walEnabled = false
			} else {
				w.walLog = log
				last, err := log.LastIndex()
				if err == nil {
					w.lastWalIndex = last
				}
				w.logger.Info("WAL initialized", "dir", cfg.WALDir, "lastIndex", w.lastWalIndex)
			}
		}
	}

	w.initShards()
	return w
}

func (w *PeriodicWriter) recoverWAL() {
	if !w.walEnabled || w.walLog == nil {
		return
	}

	first, err := w.walLog.FirstIndex()
	if err != nil {
		return
	}
	last, err := w.walLog.LastIndex()
	if err != nil {
		return
	}

	if first == 0 || last == 0 {
		return
	}

	recovered := 0
	for i := first; i <= last; i++ {
		data, err := w.walLog.Read(i)
		if err != nil {
			continue
		}

		var task PeriodicTask
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}

		_ = w.submitInternal(&task)
		recovered++
	}
	w.logger.Info("WAL recovery completed", "recovered", recovered)
}

func (w *PeriodicWriter) initShards() {
	w.shards = make([]taskShard, w.shardCount)
	for i := range w.shards {
		w.shards[i] = taskShard{
			tasks: make(map[string]*TaskGroup),
		}
	}
}

func (w *PeriodicWriter) Start() {
	go func() {
		w.recoverWAL()
	}()

	w.wg.Add(1)
	go w.flushLoop()
}

func (w *PeriodicWriter) Submit(task *PeriodicTask) error {
	if task.BatchKey == "" {
		lb := len(task.Table) + len(task.Column) + len(task.Where) + 2
		var b strings.Builder
		b.Grow(lb)
		b.WriteString(task.Table)
		b.WriteByte(':')
		b.WriteString(task.Column)
		b.WriteByte(':')
		b.WriteString(task.Where)
		task.BatchKey = b.String()
	}

	if w.walLog != nil && w.walEnabled {
		data, _ := json.Marshal(task)
		w.walMu.Lock()
		w.lastWalIndex++
		_ = w.walLog.Write(w.lastWalIndex, data)
		w.walMu.Unlock()
	}

	return w.submitInternal(task)
}

func (w *PeriodicWriter) submitInternal(task *PeriodicTask) error {
	shard := w.getShard(task.BatchKey)
	shard.mu.Lock()
	group, exists := shard.tasks[task.BatchKey]
	if !exists {
		group = &TaskGroup{
			BatchKey:  task.BatchKey,
			TaskType:  task.TaskType,
			Table:     task.Table,
			Column:    task.Column,
			Where:     task.Where,
			WhereArgs: task.WhereArgs,
		}
		shard.tasks[task.BatchKey] = group
	}
	shard.mu.Unlock()

	group.mu.Lock()
	defer group.mu.Unlock()

	switch task.TaskType {
	case TaskTypeContent:
		group.LatestTask = task
	case TaskTypeCounter:
		if group.CounterAgg == nil {
			group.CounterAgg = &CounterAggState{OpType: task.OpType}
		}
		w.aggregateCounter(group.CounterAgg, task)
	case TaskTypeMerge:
		if group.MergeAgg == nil {
			group.MergeAgg = &MergeAggState{Data: make(map[string]interface{})}
		}
		w.aggregateMerge(group.MergeAgg, task)
	}

	newCount := atomic.AddInt64(&w.taskCount, 1)
	if w.maxQueueSize > 0 && newCount >= w.maxQueueSize {
		w.triggerFlush()
	}
	return nil
}

func (w *PeriodicWriter) triggerFlush() {
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
}

func (w *PeriodicWriter) aggregateCounter(agg *CounterAggState, task *PeriodicTask) {
	delta := toFloat64(task.Delta)
	switch task.OpType {
	case OpInc:
		agg.Delta += delta
	case OpDec:
		agg.Delta -= delta
	case OpMul:
		agg.HasMul = true
		agg.MulFactor = delta
		agg.OpType = OpMul
	case OpSet:
		agg.Delta = delta
		agg.OpType = OpSet
	case OpClean:
		agg.Delta = 0
		agg.OpType = OpClean
	}
}

func (w *PeriodicWriter) aggregateMerge(agg *MergeAggState, task *PeriodicTask) {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Value, &data); err != nil {
		w.logger.Error("failed to unmarshal merge payload", "error", err)
		return
	}

	for k, v := range data {
		if existing, ok := agg.Data[k]; ok {
			if existingMap, ok1 := existing.(map[string]interface{}); ok1 {
				if newMap, ok2 := v.(map[string]interface{}); ok2 {
					agg.Data[k] = w.deepMerge(existingMap, newMap)
					continue
				}
			}
		}
		agg.Data[k] = v
	}
}

func (w *PeriodicWriter) deepMerge(base, override map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		if existing, ok := result[k]; ok {
			if existingMap, ok1 := existing.(map[string]interface{}); ok1 {
				if newMap, ok2 := v.(map[string]interface{}); ok2 {
					result[k] = w.deepMerge(existingMap, newMap)
					continue
				}
			}
		}
		result[k] = v
	}
	return result
}

func (w *PeriodicWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			w.flushAll("shutdown")
			return
		case <-ticker.C:
			w.flushAll("periodic")
		case <-w.flushCh:
			w.flushAll("queue_size")
		}
	}
}

func (w *PeriodicWriter) flushAll(trigger string) {
	if !atomic.CompareAndSwapInt32(&w.flushing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&w.flushing, 0)

	toFlush := w.drainTasks()
	atomic.SwapInt64(&w.taskCount, 0)

	if len(toFlush) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.flushTimeout)
	defer cancel()

	failed := 0
	for _, group := range toFlush {
		group.mu.Lock()
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			group.mu.Unlock()
			failed++
			continue
		}

		var execErr error
		switch group.TaskType {
		case TaskTypeContent:
			execErr = w.flushContent(ctx, tx, group)
		case TaskTypeCounter:
			execErr = w.flushCounter(ctx, tx, group)
		case TaskTypeMerge:
			execErr = w.flushMerge(ctx, tx, group)
		}

		if execErr != nil {
			_ = tx.Rollback()
			failed++
		} else {
			_ = tx.Commit()
		}
		group.mu.Unlock()
	}

	if w.walEnabled && w.walLog != nil && failed == 0 {
		w.walMu.Lock()
		_ = w.walLog.TruncateFront(w.lastWalIndex)
		w.walMu.Unlock()
	}

	atomic.AddInt64(&w.stats.flushTotal, 1)
	atomic.AddInt64(&w.stats.lastFlushUnixNano, time.Now().UnixNano())
}

func (w *PeriodicWriter) drainTasks() map[string]*TaskGroup {
	toFlush := make(map[string]*TaskGroup)
	for i := range w.shards {
		shard := &w.shards[i]
		shard.mu.Lock()
		for key, group := range shard.tasks {
			toFlush[key] = group
		}
		shard.tasks = make(map[string]*TaskGroup)
		shard.mu.Unlock()
	}
	return toFlush
}

func (w *PeriodicWriter) getShard(batchKey string) *taskShard {
	idx := int(fnv32(batchKey) % uint32(w.shardCount))
	return &w.shards[idx]
}

func (w *PeriodicWriter) flushContent(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.LatestTask == nil {
		return nil
	}
	_, err := tx.ExecContext(ctx, group.LatestTask.Query, group.LatestTask.Params...)
	return err
}

func (w *PeriodicWriter) flushCounter(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.CounterAgg == nil {
		return nil
	}

	agg := group.CounterAgg
	var query string
	var params []interface{}
	paramIdx := 1

	switch agg.OpType {
	case OpInc:
		query = fmt.Sprintf("UPDATE %s SET %s = %s + $%d", group.Table, group.Column, group.Column, paramIdx)
		params = append(params, agg.Delta)
		paramIdx++
	case OpMul:
		if agg.Delta != 0 {
			query = fmt.Sprintf("UPDATE %s SET %s = (%s + $%d) * $%d", group.Table, group.Column, group.Column, paramIdx, paramIdx+1)
			params = append(params, agg.Delta, agg.MulFactor)
			paramIdx += 2
		} else {
			query = fmt.Sprintf("UPDATE %s SET %s = %s * $%d", group.Table, group.Column, group.Column, paramIdx)
			params = append(params, agg.MulFactor)
			paramIdx++
		}
	case OpSet:
		query = fmt.Sprintf("UPDATE %s SET %s = $%d", group.Table, group.Column, paramIdx)
		params = append(params, agg.Delta)
		paramIdx++
	case OpClean:
		query = fmt.Sprintf("UPDATE %s SET %s = 0", group.Table, group.Column)
	}

	if group.Where != "" {
		query += " WHERE " + w.convertWherePlaceholders(group.Where, &paramIdx)
		params = append(params, group.WhereArgs...)
	}

	_, err := tx.ExecContext(ctx, query, params...)
	return err
}

func (w *PeriodicWriter) flushMerge(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.MergeAgg == nil || len(group.MergeAgg.Data) == 0 {
		return nil
	}

	var query string
	var params []interface{}
	paramIdx := 1

	if group.Column != "" {
		jsonData, _ := json.Marshal(group.MergeAgg.Data)
		query = fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, '{}'::jsonb) || $%d::jsonb", group.Table, group.Column, group.Column, paramIdx)
		params = append(params, string(jsonData))
		paramIdx++
	} else {
		var sets []string
		for k, v := range group.MergeAgg.Data {
			// 探测是否为 map，如果是则按 JSONB 合并
			if _, isMap := v.(map[string]interface{}); isMap {
				jsonData, _ := json.Marshal(v)
				sets = append(sets, fmt.Sprintf("%s = COALESCE(%s, '{}'::jsonb) || $%d::jsonb", k, k, paramIdx))
				params = append(params, string(jsonData))
			} else {
				sets = append(sets, fmt.Sprintf("%s = $%d", k, paramIdx))
				params = append(params, v)
			}
			paramIdx++
		}
		query = fmt.Sprintf("UPDATE %s SET %s", group.Table, strings.Join(sets, ", "))
	}

	if group.Where != "" {
		query += " WHERE " + w.convertWherePlaceholders(group.Where, &paramIdx)
		params = append(params, group.WhereArgs...)
	}

	_, err := tx.ExecContext(ctx, query, params...)
	return err
}

func (w *PeriodicWriter) convertWherePlaceholders(where string, paramIdx *int) string {
	var result strings.Builder
	for i := 0; i < len(where); i++ {
		if where[i] == '?' {
			result.WriteString(fmt.Sprintf("$%d", *paramIdx))
			*paramIdx++
		} else {
			result.WriteByte(where[i])
		}
	}
	return result.String()
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (w *PeriodicWriter) Stop() {
	w.cancel()
	w.wg.Wait()
}

func (w *PeriodicWriter) Stats() map[string]interface{} {
	return map[string]interface{}{
		"task_count":  atomic.LoadInt64(&w.taskCount),
		"is_flushing": atomic.LoadInt32(&w.flushing) == 1,
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}
