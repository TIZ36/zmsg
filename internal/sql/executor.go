package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Executor SQL执行器
type Executor struct {
	db        *sql.DB
	stmtCache *sync.Map // 预编译语句缓存
	config    *Config
	stats     *Stats
	mu        sync.RWMutex
}

// Config 执行器配置
type Config struct {
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
	ConnMaxIdleTime    time.Duration
	QueryTimeout       time.Duration
	EnableStmtCache    bool
	CacheSize          int
	SlowQueryThreshold time.Duration
	MetricsEnabled     bool
}

// Stats 执行器统计
type Stats struct {
	QueriesTotal   int64
	QueriesSuccess int64
	QueriesFailed  int64
	Transactions   int64
	SlowQueries    int64
	AvgDuration    time.Duration
	LastQueryTime  time.Time
	CacheHits      int64
	CacheMisses    int64
}

// NewExecutor 创建SQL执行器
func NewExecutor(db *sql.DB, config *Config) *Executor {
	if config == nil {
		config = &Config{
			MaxOpenConns:       25,
			MaxIdleConns:       10,
			ConnMaxLifetime:    5 * time.Minute,
			ConnMaxIdleTime:    2 * time.Minute,
			QueryTimeout:       10 * time.Second,
			EnableStmtCache:    true,
			CacheSize:          1000,
			SlowQueryThreshold: 1 * time.Second,
			MetricsEnabled:     true,
		}
	}

	// 配置数据库连接池
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	return &Executor{
		db:        db,
		stmtCache: &sync.Map{},
		config:    config,
		stats:     &Stats{},
	}
}

// Execute 执行SQL任务
func (e *Executor) Execute(ctx context.Context, task *Task) (*Result, error) {
	start := time.Now()

	// 验证任务
	if err := task.Validate(); err != nil {
		return nil, fmt.Errorf("invalid task: %w", err)
	}

	// 设置超时上下文
	if e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout)
		defer cancel()
	}

	// 准备参数
	params := make([]interface{}, len(task.Params))
	for i, param := range task.Params {
		params[i] = e.convertParam(param)
	}

	var result sql.Result
	var err error

	// 检查是否需要使用预编译语句
	if e.config.EnableStmtCache && task.TaskType == TaskTypeContent {
		result, err = e.executeWithCache(ctx, task.Query, params)
	} else {
		result, err = e.db.ExecContext(ctx, task.Query, params...)
	}

	duration := time.Since(start)

	// 更新统计
	e.updateStats(err == nil, duration)

	if err != nil {
		// 记录慢查询
		if duration >= e.config.SlowQueryThreshold {
			e.mu.Lock()
			e.stats.SlowQueries++
			e.mu.Unlock()

			// 可以记录慢查询日志
			fmt.Printf("Slow query detected: %s, duration: %v\n", task.Query, duration)
		}
		return nil, fmt.Errorf("sql execution failed: %w, query: %s", err, task.Query)
	}

	// 获取执行结果
	lastInsertID, _ := result.LastInsertId()
	rowsAffected, _ := result.RowsAffected()

	return &Result{
		LastInsertID: lastInsertID,
		RowsAffected: rowsAffected,
		Duration:     duration,
	}, nil
}

// executeWithCache 使用缓存的预编译语句执行
func (e *Executor) executeWithCache(ctx context.Context, query string, params []interface{}) (sql.Result, error) {
	// 从缓存获取预编译语句
	if stmt, ok := e.stmtCache.Load(query); ok {
		e.mu.Lock()
		e.stats.CacheHits++
		e.mu.Unlock()

		prepared := stmt.(*sql.Stmt)
		return prepared.ExecContext(ctx, params...)
	}

	e.mu.Lock()
	e.stats.CacheMisses++
	e.mu.Unlock()

	// 预编译语句
	stmt, err := e.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// 缓存预编译语句
	e.stmtCache.Store(query, stmt)

	// 清理过期的缓存（简单实现）
	if e.config.EnableStmtCache {
		e.cleanupStmtCache()
	}

	return stmt.ExecContext(ctx, params...)
}

// ExecuteBatch 批量执行SQL任务
func (e *Executor) ExecuteBatch(ctx context.Context, tasks []*Task) ([]*Result, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	// 设置超时上下文
	if e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout*time.Duration(len(tasks)))
		defer cancel()
	}

	// 开始事务
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	results := make([]*Result, len(tasks))

	// 按查询语句分组，重用预编译语句
	stmtCache := make(map[string]*sql.Stmt)

	for i, task := range tasks {
		if err := task.Validate(); err != nil {
			return nil, fmt.Errorf("invalid task at index %d: %w", i, err)
		}

		// 准备参数
		params := make([]interface{}, len(task.Params))
		for j, param := range task.Params {
			params[j] = e.convertParam(param)
		}

		// 获取或创建预编译语句
		stmt, exists := stmtCache[task.Query]
		if !exists {
			prepared, err := tx.PrepareContext(ctx, task.Query)
			if err != nil {
				return nil, fmt.Errorf("failed to prepare statement for task %d: %w", i, err)
			}
			stmt = prepared
			stmtCache[task.Query] = stmt
		}

		start := time.Now()
		result, err := stmt.ExecContext(ctx, params...)
		duration := time.Since(start)

		e.updateStats(err == nil, duration)

		if err != nil {
			return nil, fmt.Errorf("batch execution failed at index %d: %w", i, err)
		}

		lastInsertID, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()

		results[i] = &Result{
			LastInsertID: lastInsertID,
			RowsAffected: rowsAffected,
			Duration:     duration,
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 更新事务统计
	e.mu.Lock()
	e.stats.Transactions++
	e.mu.Unlock()

	return results, nil
}

// Query 查询数据
func (e *Executor) Query(ctx context.Context, task *Task) (*sql.Rows, error) {
	if err := task.Validate(); err != nil {
		return nil, fmt.Errorf("invalid task: %w", err)
	}

	// 设置超时上下文
	if e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout)
		defer cancel()
	}

	// 准备参数
	params := make([]interface{}, len(task.Params))
	for i, param := range task.Params {
		params[i] = e.convertParam(param)
	}

	start := time.Now()
	rows, err := e.db.QueryContext(ctx, task.Query, params...)
	duration := time.Since(start)

	e.updateStats(err == nil, duration)

	if err != nil {
		return nil, fmt.Errorf("query failed: %w, query: %s", err, task.Query)
	}

	return rows, nil
}

// QueryRow 查询单行数据
func (e *Executor) QueryRow(ctx context.Context, task *Task) *sql.Row {
	if err := task.Validate(); err != nil {
		// 返回一个会出错的Row
		return &sql.Row{}
	}

	// 设置超时上下文
	if e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout)
		defer cancel()
	}

	// 准备参数
	params := make([]interface{}, len(task.Params))
	for i, param := range task.Params {
		params[i] = e.convertParam(param)
	}

	start := time.Now()
	row := e.db.QueryRowContext(ctx, task.Query, params...)
	duration := time.Since(start)

	e.updateStats(true, duration) // 假设成功，实际错误会在Scan时返回

	return row
}

// QueryToMap 查询数据并转换为map
func (e *Executor) QueryToMap(ctx context.Context, task *Task) ([]map[string]interface{}, error) {
	rows, err := e.Query(ctx, task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		// 创建值的切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// 转换为map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// 处理数据库特殊类型
			if b, ok := val.([]byte); ok {
				// 尝试解码为JSON
				var jsonVal interface{}
				if json.Unmarshal(b, &jsonVal) == nil {
					rowMap[col] = jsonVal
				} else {
					rowMap[col] = string(b)
				}
			} else {
				rowMap[col] = val
			}
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

// QueryToStruct 查询数据并转换为结构体
func (e *Executor) QueryToStruct(ctx context.Context, task *Task, dest interface{}) error {
	rows, err := e.Query(ctx, task)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	sliceVal := destVal.Elem()
	elemType := sliceVal.Type().Elem()

	for rows.Next() {
		// 创建结构体实例
		elemPtr := reflect.New(elemType)
		elem := elemPtr.Elem()

		// 获取字段映射
		fieldMap := make(map[string]interface{})
		for i := 0; i < elem.NumField(); i++ {
			field := elem.Type().Field(i)
			tag := field.Tag.Get("db")
			if tag == "" {
				tag = strings.ToLower(field.Name)
			}
			fieldMap[tag] = elem.Field(i).Addr().Interface()
		}

		// 创建扫描值
		scanValues := make([]interface{}, len(columns))
		for i, col := range columns {
			if field, ok := fieldMap[col]; ok {
				scanValues[i] = field
			} else {
				// 使用临时变量存储未映射的列
				var temp interface{}
				scanValues[i] = &temp
			}
		}

		if err := rows.Scan(scanValues...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// 添加到切片
		sliceVal = reflect.Append(sliceVal, elem)
	}

	destVal.Elem().Set(sliceVal)

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

// convertParam 转换参数类型
func (e *Executor) convertParam(param interface{}) interface{} {
	if param == nil {
		return nil
	}

	// 检查是否已经是支持的类型
	switch v := param.(type) {
	case []byte:
		return v // PostgreSQL 直接支持 []byte
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return v
	case time.Time:
		return v
	case *time.Time:
		if v == nil {
			return nil
		}
		return *v
	case sql.NullString, sql.NullInt64, sql.NullFloat64, sql.NullBool, sql.NullTime:
		return v
	case json.RawMessage:
		return []byte(v)
	}

	// 使用反射处理复杂类型
	v := reflect.ValueOf(param)

	// 处理切片
	if v.Kind() == reflect.Slice {
		// 特殊处理 []byte
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return param
		}

		// 转换为 PostgreSQL 数组格式
		return e.convertSlice(param)
	}

	// 处理 map 和结构体 -> JSONB
	if v.Kind() == reflect.Map || v.Kind() == reflect.Struct {
		jsonBytes, err := json.Marshal(param)
		if err == nil {
			return jsonBytes
		}
	}

	// 处理指针
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return e.convertParam(v.Elem().Interface())
	}

	// 默认转换为字符串
	return fmt.Sprintf("%v", param)
}

// convertSlice 转换切片为PostgreSQL数组格式
func (e *Executor) convertSlice(param interface{}) interface{} {
	v := reflect.ValueOf(param)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return param
	}

	length := v.Len()
	if length == 0 {
		return "{}"
	}

	var builder strings.Builder
	builder.WriteString("{")

	for i := 0; i < length; i++ {
		if i > 0 {
			builder.WriteString(",")
		}

		elem := v.Index(i).Interface()

		switch val := elem.(type) {
		case string:
			// 转义双引号
			escaped := strings.ReplaceAll(val, "\"", "\"\"")
			builder.WriteString(fmt.Sprintf("\"%s\"", escaped))
		case []byte:
			builder.WriteString(fmt.Sprintf("\"\\\\x%x\"", val))
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64, bool:
			builder.WriteString(fmt.Sprintf("%v", val))
		default:
			// 递归处理嵌套类型
			converted := e.convertParam(val)
			builder.WriteString(fmt.Sprintf("%v", converted))
		}
	}

	builder.WriteString("}")
	return builder.String()
}

// updateStats 更新统计信息
func (e *Executor) updateStats(success bool, duration time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stats.QueriesTotal++
	e.stats.LastQueryTime = time.Now()

	if success {
		e.stats.QueriesSuccess++
	} else {
		e.stats.QueriesFailed++
	}

	// 更新平均耗时
	if e.stats.QueriesSuccess > 1 {
		total := e.stats.AvgDuration * time.Duration(e.stats.QueriesSuccess-1)
		e.stats.AvgDuration = (total + duration) / time.Duration(e.stats.QueriesSuccess)
	} else if success {
		e.stats.AvgDuration = duration
	}

	// 记录慢查询
	if duration >= e.config.SlowQueryThreshold {
		e.stats.SlowQueries++
	}
}

// cleanupStmtCache 清理预编译语句缓存
func (e *Executor) cleanupStmtCache() {
	// 简单的LRU清理策略
	// 这里可以扩展为更复杂的清理策略
	if e.config.CacheSize > 0 {
		var keys []string
		var count int

		e.stmtCache.Range(func(key, value interface{}) bool {
			keys = append(keys, key.(string))
			count++
			return count <= e.config.CacheSize*2
		})

		// 如果超过限制，清理一部分
		if count > e.config.CacheSize {
			for i := 0; i < count-e.config.CacheSize; i++ {
				if stmt, ok := e.stmtCache.Load(keys[i]); ok {
					stmt.(*sql.Stmt).Close()
					e.stmtCache.Delete(keys[i])
				}
			}
		}
	}
}

// GetStats 获取统计信息
func (e *Executor) GetStats() *Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := *e.stats
	return &stats
}

// ResetStats 重置统计
func (e *Executor) ResetStats() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stats = &Stats{}
}

// Ping 检查数据库连接
func (e *Executor) Ping(ctx context.Context) error {
	return e.db.PingContext(ctx)
}

// Close 关闭执行器
func (e *Executor) Close() error {
	// 清理预编译语句缓存
	e.stmtCache.Range(func(key, value interface{}) bool {
		if stmt, ok := value.(*sql.Stmt); ok {
			stmt.Close()
		}
		e.stmtCache.Delete(key)
		return true
	})

	return e.db.Close()
}
