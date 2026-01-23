package batch

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TaskType 任务类型
type TaskType int

const (
	TaskTypeContent TaskType = iota // 覆盖（只执行最新）
	TaskTypeCounter                 // 计数器累加
	TaskTypeAppend                  // Slice 追加
	TaskTypePut                     // Map 写入
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
	Table  string // 表名
	Column string // 列名
	Where  string // WHERE 条件（不含 WHERE 关键字）
	WhereArgs []interface{} // WHERE 参数

	// Counter 聚合
	OpType    OpType      // 操作类型
	Delta     interface{} // 增量值（int64, float64）

	// Slice 聚合
	SliceValue interface{} // 要追加或删除的值

	// Map 聚合
	MapKey   string      // Map 键
	MapValue interface{} // Map 值
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
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	flushCh   chan struct{} // flush 信号通道
	flushing  int32         // 是否正在 flush（原子操作）

	stats writerStats
}

// TaskGroup 任务组（按 BatchKey 聚合）
type TaskGroup struct {
	BatchKey string
	TaskType TaskType
	Table    string
	Column   string
	Where    string
	WhereArgs []interface{}

	// Content 类型：只保留最新的任务
	LatestTask *PeriodicTask

	// Counter 聚合状态
	CounterAgg *CounterAggState

	// Slice 聚合状态
	SliceAgg *SliceAggState

	// Map 聚合状态
	MapAgg *MapAggState

	mu sync.Mutex
}

type taskShard struct {
	mu    sync.RWMutex
	tasks map[string]*TaskGroup
}

type writerStats struct {
	flushTotal        int64
	flushFailedGroups int64
	flushDurationNanos int64
	lastFlushUnixNano int64
}

// CounterAggState 计数器聚合状态（用于周期写内存聚合）
type CounterAggState struct {
	Delta     float64 // 累加的增量（统一用 float64）
	OpType    OpType  // 最终操作类型
	HasMul    bool    // 是否有乘法操作
	MulFactor float64 // 乘法因子
}

// SliceAggState Slice 聚合状态（用于周期写内存聚合）
type SliceAggState struct {
	ToAdd []interface{} // 要追加的元素
	ToDel []interface{} // 要删除的元素
}

// MapAggState Map 聚合状态（用于周期写内存聚合）
type MapAggState struct {
	ToSet map[string]interface{} // 要设置的键值对
	ToDel []string               // 要删除的键
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
	MaxQueueSize  int64         // 队列长度阈值（达到后立即 flush）
	ShardCount    int           // 分片数量
	FlushTimeout  time.Duration // flush 超时
}

// DefaultWriterConfig 默认配置
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		FlushInterval: 5 * time.Second,
		MaxQueueSize:  1000, // 默认 1000 个任务触发 flush
		ShardCount:    16,
		FlushTimeout:  30 * time.Second,
	}
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
		flushCh:       make(chan struct{}, 1), // 带缓冲，避免阻塞
	}

	w.initShards()
	return w
}

func (w *PeriodicWriter) initShards() {
	w.shards = make([]taskShard, w.shardCount)
	for i := range w.shards {
		w.shards[i] = taskShard{
			tasks: make(map[string]*TaskGroup),
		}
	}
}

// Start 启动周期写入
func (w *PeriodicWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
	w.logger.Info("periodic writer started", 
		"interval", w.flushInterval, 
		"maxQueueSize", w.maxQueueSize)
}

// Submit 提交周期任务
func (w *PeriodicWriter) Submit(task *PeriodicTask) error {
	if task.BatchKey == "" {
		task.BatchKey = fmt.Sprintf("%s:%s:%s", task.Table, task.Column, task.Where)
	}

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

	switch task.TaskType {
	case TaskTypeContent:
		// Content: 只保留最新的
		group.LatestTask = task

	case TaskTypeCounter:
		// Counter: 在内存中聚合
		if group.CounterAgg == nil {
			group.CounterAgg = &CounterAggState{
				Delta:  0,
				OpType: task.OpType,
			}
		}
		w.aggregateCounter(group.CounterAgg, task)

	case TaskTypeAppend:
		// Slice: 收集所有操作
		if group.SliceAgg == nil {
			group.SliceAgg = &SliceAggState{
				ToAdd: make([]interface{}, 0),
				ToDel: make([]interface{}, 0),
			}
		}
		w.aggregateSlice(group.SliceAgg, task)

	case TaskTypePut:
		// Map: 收集所有操作
		if group.MapAgg == nil {
			group.MapAgg = &MapAggState{
				ToSet: make(map[string]interface{}),
				ToDel: make([]string, 0),
			}
		}
		w.aggregateMap(group.MapAgg, task)
	}

	group.mu.Unlock()

	// 增加任务计数并检查是否需要触发 flush
	newCount := atomic.AddInt64(&w.taskCount, 1)
	if w.maxQueueSize > 0 && newCount >= w.maxQueueSize {
		w.triggerFlush()
	}

	return nil
}

// triggerFlush 触发 flush（非阻塞）
func (w *PeriodicWriter) triggerFlush() {
	// 非阻塞发送，如果通道已满说明已经有 flush 在等待
	select {
	case w.flushCh <- struct{}{}:
		w.logger.Debug("flush triggered by queue size", "count", atomic.LoadInt64(&w.taskCount))
	default:
		// 已经有 flush 信号在等待，忽略
	}
}

// aggregateCounter Counter 聚合
func (w *PeriodicWriter) aggregateCounter(agg *CounterAggState, task *PeriodicTask) {
	delta := toFloat64(task.Delta)
	opNames := []string{"Inc", "Dec", "Mul", "Set", "Clean", "Add", "Del"}
	opName := "Unknown"
	if int(task.OpType) < len(opNames) {
		opName = opNames[task.OpType]
	}
	prevDelta := agg.Delta

	switch task.OpType {
	case OpInc:
		agg.Delta += delta
		if !agg.HasMul {
			agg.OpType = OpInc
		}
	case OpDec:
		agg.Delta -= delta
		if !agg.HasMul {
			agg.OpType = OpInc // 减法也是增量操作
		}
	case OpMul:
		// 乘法需要特殊处理：先应用之前的增量，再乘
		agg.HasMul = true
		agg.MulFactor = delta
		agg.OpType = OpMul
	case OpSet:
		// 直接设置覆盖之前所有聚合
		agg.Delta = delta
		agg.HasMul = false
		agg.OpType = OpSet
	case OpClean:
		// 清零覆盖之前所有聚合
		agg.Delta = 0
		agg.HasMul = false
		agg.OpType = OpClean
	}

	w.logger.Debug("[Batch/Counter] aggregate", 
		"op", opName,
		"inputDelta", delta,
		"prevAggDelta", prevDelta,
		"newAggDelta", agg.Delta,
		"batchKey", task.BatchKey)
}

// aggregateSlice Slice 聚合
func (w *PeriodicWriter) aggregateSlice(agg *SliceAggState, task *PeriodicTask) {
	switch task.OpType {
	case OpAdd:
		agg.ToAdd = append(agg.ToAdd, task.SliceValue)
		w.logger.Debug("[Batch/Slice] aggregate Add", 
			"value", task.SliceValue,
			"totalToAdd", len(agg.ToAdd),
			"batchKey", task.BatchKey)
	case OpDel:
		agg.ToDel = append(agg.ToDel, task.SliceValue)
		w.logger.Debug("[Batch/Slice] aggregate Del", 
			"value", task.SliceValue,
			"totalToDel", len(agg.ToDel),
			"batchKey", task.BatchKey)
	case OpClean:
		// 清空时，清除之前的 add 操作
		agg.ToAdd = nil
		agg.ToDel = nil
		w.logger.Debug("[Batch/Slice] aggregate Clean", "batchKey", task.BatchKey)
	}
}

// aggregateMap Map 聚合
func (w *PeriodicWriter) aggregateMap(agg *MapAggState, task *PeriodicTask) {
	switch task.OpType {
	case OpSet:
		// 如果之前要删除这个 key，取消删除
		for i, k := range agg.ToDel {
			if k == task.MapKey {
				agg.ToDel = append(agg.ToDel[:i], agg.ToDel[i+1:]...)
				break
			}
		}
		agg.ToSet[task.MapKey] = task.MapValue
		w.logger.Debug("[Batch/Map] aggregate Set", 
			"mapKey", task.MapKey,
			"mapValue", task.MapValue,
			"totalToSet", len(agg.ToSet),
			"batchKey", task.BatchKey)
	case OpDel:
		// 如果之前要设置这个 key，取消设置
		delete(agg.ToSet, task.MapKey)
		agg.ToDel = append(agg.ToDel, task.MapKey)
		w.logger.Debug("[Batch/Map] aggregate Del", 
			"mapKey", task.MapKey,
			"totalToDel", len(agg.ToDel),
			"batchKey", task.BatchKey)
	}
}

// flushLoop 定时 flush 循环
// 支持两种触发机制：
//   1. 周期触发：定时 flush
//   2. 长度触发：队列长度达到阈值时立即 flush
func (w *PeriodicWriter) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// 关闭时 flush 所有剩余数据
			w.flushAll("shutdown")
			return

		case <-ticker.C:
			// 周期触发
			w.flushAll("periodic")

		case <-w.flushCh:
			// 长度触发（高并发场景）
			w.flushAll("queue_size")
		}
	}
}

// flushAll flush 所有数据
// trigger: 触发原因（"periodic", "queue_size", "shutdown"）
func (w *PeriodicWriter) flushAll(trigger string) {
	// 使用 CAS 防止并发 flush
	if !atomic.CompareAndSwapInt32(&w.flushing, 0, 1) {
		w.logger.Debug("[Batch/Flush] skipped (already flushing)", "trigger", trigger)
		return
	}
	defer atomic.StoreInt32(&w.flushing, 0)

	start := time.Now()
	toFlush := w.drainTasks()
	prevCount := atomic.SwapInt64(&w.taskCount, 0) // 重置计数

	if len(toFlush) == 0 {
		w.logger.Debug("[Batch/Flush] no tasks to flush", "trigger", trigger)
		return
	}

	w.logger.Debug("[Batch/Flush] START", 
		"trigger", trigger,
		"groups", len(toFlush),
		"taskCount", prevCount)

	ctx, cancel := context.WithTimeout(context.Background(), w.flushTimeout)
	defer cancel()

	flushed := 0
	failed := 0
	typeNames := []string{"Content", "Counter", "Slice", "Map"}
	
	// 每个 group 独立事务，避免一个失败导致全部失败
	for _, group := range toFlush {
		group.mu.Lock()
		
		typeName := "Unknown"
		if int(group.TaskType) < len(typeNames) {
			typeName = typeNames[group.TaskType]
		}
		
		// 为每个 group 创建独立事务
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			w.logger.Error("[Batch/Flush] begin tx failed", 
				"error", err, 
				"batchKey", group.BatchKey)
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
		case TaskTypeAppend:
			execErr = w.flushSlice(ctx, tx, group)
		case TaskTypePut:
			execErr = w.flushMap(ctx, tx, group)
		}

		group.mu.Unlock()

		if execErr != nil {
			tx.Rollback()
			w.logger.Error("[Batch/Flush] group failed (rollback)", 
				"error", execErr, 
				"batchKey", group.BatchKey,
				"type", typeName)
			failed++
			continue
		}
		
		// 提交事务
		if err := tx.Commit(); err != nil {
			w.logger.Error("[Batch/Flush] commit failed", 
				"error", err, 
				"batchKey", group.BatchKey,
				"type", typeName)
			failed++
			continue
		}
		
		w.logger.Debug("[Batch/Flush] group success", 
			"batchKey", group.BatchKey,
			"type", typeName,
			"table", group.Table,
			"column", group.Column)
		flushed++
	}

	w.logger.Info("[Batch/Flush] COMPLETED", 
		"trigger", trigger,
		"groupsFlushed", flushed, 
		"groupsFailed", failed,
		"totalGroups", len(toFlush),
		"taskCount", prevCount)

	atomic.AddInt64(&w.stats.flushTotal, 1)
	if failed > 0 {
		atomic.AddInt64(&w.stats.flushFailedGroups, int64(failed))
	}
	atomic.AddInt64(&w.stats.flushDurationNanos, time.Since(start).Nanoseconds())
	atomic.StoreInt64(&w.stats.lastFlushUnixNano, time.Now().UnixNano())
}

func (w *PeriodicWriter) drainTasks() map[string]*TaskGroup {
	toFlush := make(map[string]*TaskGroup)
	for i := range w.shards {
		shard := &w.shards[i]
		shard.mu.Lock()
		if len(shard.tasks) > 0 {
			for key, group := range shard.tasks {
				toFlush[key] = group
			}
			shard.tasks = make(map[string]*TaskGroup)
		}
		shard.mu.Unlock()
	}
	return toFlush
}

func (w *PeriodicWriter) getShard(batchKey string) *taskShard {
	if w.shardCount <= 1 {
		return &w.shards[0]
	}
	idx := int(fnv32(batchKey) % uint32(w.shardCount))
	return &w.shards[idx]
}

// flushContent 刷新 Content 类型
func (w *PeriodicWriter) flushContent(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.LatestTask == nil {
		return nil
	}
	w.logger.Debug("[Batch/DB] exec Content SQL", 
		"query", group.LatestTask.Query, 
		"params", group.LatestTask.Params,
		"batchKey", group.BatchKey)
	result, err := tx.ExecContext(ctx, group.LatestTask.Query, group.LatestTask.Params...)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	w.logger.Debug("[Batch/DB] Content SQL success", "rowsAffected", rows, "batchKey", group.BatchKey)
	return nil
}

// flushCounter 刷新 Counter 类型（聚合后一次执行）
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
		// UPDATE table SET column = column + $1 WHERE ...
		query = fmt.Sprintf("UPDATE %s SET %s = %s + $%d",
			group.Table, group.Column, group.Column, paramIdx)
		params = append(params, agg.Delta)
		paramIdx++
	case OpMul:
		// UPDATE table SET column = column * $1 WHERE ...
		// 如果有之前的增量，需要先加再乘：(column + delta) * factor
		if agg.Delta != 0 {
			query = fmt.Sprintf("UPDATE %s SET %s = (%s + $%d) * $%d",
				group.Table, group.Column, group.Column, paramIdx, paramIdx+1)
			params = append(params, agg.Delta, agg.MulFactor)
			paramIdx += 2
		} else {
			query = fmt.Sprintf("UPDATE %s SET %s = %s * $%d",
				group.Table, group.Column, group.Column, paramIdx)
			params = append(params, agg.MulFactor)
			paramIdx++
		}
	case OpSet:
		// UPDATE table SET column = $1 WHERE ...
		query = fmt.Sprintf("UPDATE %s SET %s = $%d",
			group.Table, group.Column, paramIdx)
		params = append(params, agg.Delta)
		paramIdx++
	case OpClean:
		// UPDATE table SET column = 0 WHERE ...
		query = fmt.Sprintf("UPDATE %s SET %s = 0", group.Table, group.Column)
	}

	// 添加 WHERE 子句
	if group.Where != "" {
		query += " WHERE " + w.convertWherePlaceholders(group.Where, &paramIdx)
		params = append(params, group.WhereArgs...)
	}

	w.logger.Debug("[Batch/DB] exec Counter SQL", 
		"query", query, 
		"params", params,
		"aggDelta", agg.Delta,
		"batchKey", group.BatchKey)
	result, err := tx.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	w.logger.Debug("[Batch/DB] Counter SQL success", "rowsAffected", rows, "batchKey", group.BatchKey)
	return nil
}

// flushSlice 刷新 Slice 类型（聚合后一次执行）
func (w *PeriodicWriter) flushSlice(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.SliceAgg == nil {
		return nil
	}

	agg := group.SliceAgg
	paramIdx := 1
	var params []interface{}

	// 构建合并的 SQL
	// 先处理删除，再处理追加
	var setClauses []string

	if len(agg.ToDel) > 0 {
		// 删除：使用 jsonb_agg + filter
		// column = (SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb) FROM jsonb_array_elements(column) AS elem WHERE elem NOT IN ($1, $2, ...))
		delPlaceholders := make([]string, len(agg.ToDel))
		for i, v := range agg.ToDel {
			delPlaceholders[i] = fmt.Sprintf("$%d::jsonb", paramIdx)
			jsonVal, _ := json.Marshal(v)
			params = append(params, string(jsonVal))
			paramIdx++
		}
		setClauses = append(setClauses, fmt.Sprintf(
			`%s = (SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb) FROM jsonb_array_elements(%s) AS elem WHERE elem NOT IN (%s))`,
			group.Column, group.Column, strings.Join(delPlaceholders, ", ")))
	}

	if len(agg.ToAdd) > 0 {
		// 追加：column = COALESCE(column, '[]'::jsonb) || $1::jsonb
		jsonArr, _ := json.Marshal(agg.ToAdd)
		if len(setClauses) > 0 {
			// 如果有删除操作，追加到删除结果上
			setClauses[0] = strings.Replace(setClauses[0], 
				fmt.Sprintf("%s = ", group.Column),
				fmt.Sprintf("%s = (", group.Column), 1) + fmt.Sprintf(") || $%d::jsonb", paramIdx)
		} else {
			setClauses = append(setClauses, fmt.Sprintf(
				"%s = COALESCE(%s, '[]'::jsonb) || $%d::jsonb",
				group.Column, group.Column, paramIdx))
		}
		params = append(params, string(jsonArr))
		paramIdx++
	}

	if len(setClauses) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s SET %s", group.Table, strings.Join(setClauses, ", "))

	// 添加 WHERE 子句
	if group.Where != "" {
		query += " WHERE " + w.convertWherePlaceholders(group.Where, &paramIdx)
		params = append(params, group.WhereArgs...)
	}

	w.logger.Debug("[Batch/DB] exec Slice SQL", 
		"query", query, 
		"params", params,
		"toAddCount", len(agg.ToAdd),
		"toDelCount", len(agg.ToDel),
		"batchKey", group.BatchKey)
	result, err := tx.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	w.logger.Debug("[Batch/DB] Slice SQL success", "rowsAffected", rows, "batchKey", group.BatchKey)
	return nil
}

// flushMap 刷新 Map 类型（聚合后一次执行）
func (w *PeriodicWriter) flushMap(ctx context.Context, tx *sql.Tx, group *TaskGroup) error {
	if group.MapAgg == nil {
		return nil
	}

	agg := group.MapAgg
	paramIdx := 1
	var params []interface{}
	var setClauses []string

	// 先处理删除
	if len(agg.ToDel) > 0 {
		// column = column - 'key1' - 'key2' ...
		delExpr := group.Column
		for _, k := range agg.ToDel {
			delExpr = fmt.Sprintf("%s - $%d", delExpr, paramIdx)
			params = append(params, k)
			paramIdx++
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", group.Column, delExpr))
	}

	// 再处理设置
	if len(agg.ToSet) > 0 {
		// column = COALESCE(column, '{}'::jsonb) || jsonb_build_object($1, $2::jsonb, $3, $4::jsonb, ...)
		var kvPairs []string
		for k, v := range agg.ToSet {
			kvPairs = append(kvPairs, fmt.Sprintf("$%d", paramIdx))
			params = append(params, k)
			paramIdx++
			
			jsonVal, _ := json.Marshal(v)
			kvPairs = append(kvPairs, fmt.Sprintf("$%d::jsonb", paramIdx))
			params = append(params, string(jsonVal))
			paramIdx++
		}

		setExpr := group.Column
		if len(setClauses) > 0 {
			// 如果有删除操作，在删除结果上合并
			setExpr = fmt.Sprintf("(%s)", strings.TrimPrefix(setClauses[0], group.Column+" = "))
			setClauses = setClauses[:0] // 清空，下面重建
		}

		setClauses = append(setClauses, fmt.Sprintf(
			"%s = COALESCE(%s, '{}'::jsonb) || jsonb_build_object(%s)",
			group.Column, setExpr, strings.Join(kvPairs, ", ")))
	}

	if len(setClauses) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s SET %s", group.Table, strings.Join(setClauses, ", "))

	// 添加 WHERE 子句
	if group.Where != "" {
		query += " WHERE " + w.convertWherePlaceholders(group.Where, &paramIdx)
		params = append(params, group.WhereArgs...)
	}

	w.logger.Debug("[Batch/DB] exec Map SQL", 
		"query", query, 
		"params", params,
		"toSetCount", len(agg.ToSet),
		"toDelCount", len(agg.ToDel),
		"batchKey", group.BatchKey)
	result, err := tx.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	w.logger.Debug("[Batch/DB] Map SQL success", "rowsAffected", rows, "batchKey", group.BatchKey)
	return nil
}

// convertWherePlaceholders 将 WHERE 子句中的 ? 转换为 $N
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

// Stop 停止写入器
func (w *PeriodicWriter) Stop() {
	w.cancel()
	w.wg.Wait()
	w.logger.Info("periodic writer stopped")
}

// Stats 获取统计信息
func (w *PeriodicWriter) Stats() map[string]interface{} {
	counterGroups := 0
	sliceGroups := 0
	mapGroups := 0
	contentGroups := 0

	totalGroups := 0
	for i := range w.shards {
		shard := &w.shards[i]
		shard.mu.RLock()
		for _, group := range shard.tasks {
			totalGroups++
			switch group.TaskType {
			case TaskTypeCounter:
				counterGroups++
			case TaskTypeAppend:
				sliceGroups++
			case TaskTypePut:
				mapGroups++
			case TaskTypeContent:
				contentGroups++
			}
		}
		shard.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_groups":    totalGroups,
		"counter_groups":  counterGroups,
		"slice_groups":    sliceGroups,
		"map_groups":      mapGroups,
		"content_groups":  contentGroups,
		"task_count":      atomic.LoadInt64(&w.taskCount),
		"max_queue_size":  w.maxQueueSize,
		"flush_interval":  w.flushInterval.String(),
		"flush_timeout":   w.flushTimeout.String(),
		"is_flushing":     atomic.LoadInt32(&w.flushing) == 1,
		"flush_total":     atomic.LoadInt64(&w.stats.flushTotal),
		"flush_failed_groups": atomic.LoadInt64(&w.stats.flushFailedGroups),
		"flush_total_duration_ns": atomic.LoadInt64(&w.stats.flushDurationNanos),
		"last_flush_unix_nano": atomic.LoadInt64(&w.stats.lastFlushUnixNano),
	}
}

// toFloat64 将各种数值类型转换为 float64
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}
