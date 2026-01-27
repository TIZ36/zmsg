package zmsg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// tableBuilder 实现 TableBuilder 接口
type tableBuilder struct {
	z            *zmsg
	table        string
	cacheKey     string
	serializeKey string
	strategy     PeriodicStrategy
	where        string
	whereArgs    []any
}

func (b *tableBuilder) CacheKey(key string) TableBuilder {
	b.cacheKey = key
	return b
}

func (b *tableBuilder) Serialize(key string) TableBuilder {
	b.serializeKey = key
	return b
}

func (b *tableBuilder) Where(condition string, args ...any) TableBuilder {
	b.where = condition
	b.whereArgs = args
	return b
}

func (b *tableBuilder) PeriodicOverride() TableBuilder {
	b.strategy = StrategyOverride
	return b
}

func (b *tableBuilder) PeriodicMerge() TableBuilder {
	b.strategy = StrategyMerge
	return b
}

func (b *tableBuilder) PeriodicCount() TableBuilder {
	b.strategy = StrategyCount
	return b
}

func (b *tableBuilder) Save(data any) error {
	ctx := context.Background()
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// 简单实现：将 struct 映射为 INSERT ... ON CONFLICT DO UPDATE
	// 生产建议：使用更成熟的 ORM 映射或由用户提供 SQL 模板
	sqlTask, err := b.buildSaveTask(data, jsonData)
	if err != nil {
		return err
	}

	if b.serializeKey != "" {
		sqlTask.SerializeKey = b.serializeKey
		// TODO: 路由到顺序写逻辑
		return b.z.cacheAndDelayStore(ctx, b.cacheKey, jsonData, sqlTask)
	}

	switch b.strategy {
	case StrategyOverride:
		sqlTask.TaskType = TaskTypeContent
		return b.z.cacheAndPeriodicStore(ctx, b.cacheKey, jsonData, sqlTask)
	case StrategyMerge:
		sqlTask.TaskType = TaskTypeMerge
		return b.z.cacheAndPeriodicStore(ctx, b.cacheKey, jsonData, sqlTask)
	case StrategyNormal:
		_, err := b.z.cacheAndStore(ctx, b.cacheKey, jsonData, sqlTask)
		return err
	default:
		return fmt.Errorf("unsupported strategy for Save")
	}
}

func (b *tableBuilder) UpdateColumns(columns map[string]any) error {
	ctx := context.Background()
	// 构建 UPDATE SQL
	sqlTask := b.buildUpdateColumnsTask(columns)

	if b.serializeKey != "" {
		sqlTask.SerializeKey = b.serializeKey
		// 对于 UpdateColumns，我们需要序列化数据，如果是 map 则序列化为 JSON
		jsonData, _ := json.Marshal(columns)
		return b.z.cacheAndDelayStore(ctx, b.cacheKey, jsonData, sqlTask)
	}

	// 如果是 Merge 策略，我们可以利用我们的聚合 Merge 特性
	if b.strategy == StrategyMerge {
		jsonData, _ := json.Marshal(columns)
		sqlTask.TaskType = TaskTypeMerge
		return b.z.cacheAndPeriodicStore(ctx, b.cacheKey, jsonData, sqlTask)
	}

	_, err := b.z.execSQL(ctx, sqlTask)
	return err
}

func (b *tableBuilder) UpdateColumn() ColumnBuilder {
	return &columnBuilder{tb: b}
}

func (b *tableBuilder) Del() error {
	ctx := context.Background()

	// 优先使用显式设置的 where，如果没有则 fallback 到 cacheKey
	where := b.where
	whereArgs := b.whereArgs
	if where == "" && b.cacheKey != "" {
		where = "id = ?"
		whereArgs = []any{b.cacheKey}
	}

	sql := fmt.Sprintf("DELETE FROM %s", b.table)
	if where != "" {
		sql += " WHERE " + where
	}

	task := &SQLTask{
		Query:    convertPlaceholders(sql),
		Params:   whereArgs,
		Table:    b.table,
		Where:    where,
		BatchKey: fmt.Sprintf("%s:%s", b.table, where),
	}
	return b.z.delStore(ctx, b.cacheKey, task)
}

func (b *tableBuilder) Query() ([]byte, error) {
	ctx := context.Background()
	return b.z.get(ctx, b.cacheKey)
}

// buildSaveTask 构建通用的 INSERT ON CONFLICT SQL
func (b *tableBuilder) buildSaveTask(data any, jsonData []byte) (*SQLTask, error) {
	v := reflect.Indirect(reflect.ValueOf(data))
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Save data must be a struct or pointer to struct")
	}

	t := v.Type()
	var cols []string
	var vals []any
	var conflictCols []string
	var updateSets []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}

		parts := strings.Split(tag, ",")
		colName := parts[0]
		cols = append(cols, colName)
		vals = append(vals, v.Field(i).Interface())

		isPK := false
		for _, p := range parts[1:] {
			if p == "pk" {
				isPK = true
				conflictCols = append(conflictCols, colName)
			}
		}
		if !isPK {
			updateSets = append(updateSets, fmt.Sprintf("%s = EXCLUDED.%s", colName, colName))
		}
	}

	if len(conflictCols) == 0 {
		return nil, fmt.Errorf("no primary key defined in struct tags (use `db:\"id,pk\"`)")
	}

	var query string
	if len(updateSets) > 0 {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			b.table,
			strings.Join(cols, ", "),
			strings.Repeat("?, ", len(cols)-1)+"?",
			strings.Join(conflictCols, ", "),
			strings.Join(updateSets, ", "),
		)
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			b.table,
			strings.Join(cols, ", "),
			strings.Repeat("?, ", len(cols)-1)+"?",
			strings.Join(conflictCols, ", "),
		)
	}

	var whereParts []string
	for _, col := range conflictCols {
		whereParts = append(whereParts, fmt.Sprintf("%s = ?", col))
	}
	where := strings.Join(whereParts, " AND ")

	return &SQLTask{
		Query:     convertPlaceholders(query),
		Params:    vals,
		Table:     b.table,
		Where:     where,
		WhereArgs: vals, // 对于 Save，pk 对应的值在 vals 中（简单假设主键先排序或由用户保证）
		BatchKey:  fmt.Sprintf("%s:%s:%v", b.table, where, vals),
	}, nil
}

func (b *tableBuilder) buildUpdateColumnsTask(columns map[string]any) *SQLTask {
	var sets []string
	var params []any
	for k, v := range columns {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		params = append(params, v)
	}

	// 优先使用显式设置的 where，如果没有则 fallback 到 cacheKey
	where := b.where
	whereArgs := b.whereArgs
	if where == "" && b.cacheKey != "" {
		where = "id = ?"
		whereArgs = []any{b.cacheKey}
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", b.table, strings.Join(sets, ", "), where)
	params = append(params, whereArgs...)

	return &SQLTask{
		Query:     convertPlaceholders(query),
		Params:    params,
		Table:     b.table,
		Where:     where,
		WhereArgs: whereArgs,
		BatchKey:  fmt.Sprintf("%s:%s:%v", b.table, where, whereArgs),
	}
}

// columnBuilder 实现 ColumnBuilder 接口
type columnBuilder struct {
	tb     *tableBuilder
	column string
}

func (b *columnBuilder) Column(name string) ColumnBuilder {
	b.column = name
	return b
}

func (b *columnBuilder) Do(fn ComputeFunc, delta any) error {
	ctx := context.Background()

	// 优先使用显式设置的 where，如果没有则 fallback 到 cacheKey
	where := b.tb.where
	whereArgs := b.tb.whereArgs
	if where == "" && b.tb.cacheKey != "" {
		where = "id = ?"
		whereArgs = []any{b.tb.cacheKey}
	}

	task := &SQLTask{
		Table:     b.tb.table,
		Column:    b.column,
		Delta:     delta,
		Where:     where,
		WhereArgs: whereArgs,
		BatchKey:  fmt.Sprintf("%s:%s:%s:%v", b.tb.table, b.column, where, whereArgs),
	}

	if b.tb.serializeKey != "" {
		task.SerializeKey = b.tb.serializeKey
		// 构建 SQL
		updateSQL := fmt.Sprintf("UPDATE %s SET %s = %s + ? WHERE %s", b.tb.table, b.column, b.column, where)
		if b.tb.strategy == StrategyMerge {
			updateSQL = fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, '{}'::jsonb) || ?::jsonb WHERE %s", b.tb.table, b.column, b.column, where)
		}
		task.Query = convertPlaceholders(updateSQL)
		task.Params = append([]any{delta}, whereArgs...)

		jsonData, _ := json.Marshal(delta)
		return b.tb.z.cacheAndDelayStore(ctx, b.tb.cacheKey, jsonData, task)
	}

	switch b.tb.strategy {
	case StrategyCount:
		task.TaskType = TaskTypeCounter
		task.OpType = OpInc
		return b.tb.z.cacheAndPeriodicStore(ctx, b.tb.cacheKey, nil, task)
	case StrategyMerge:
		task.TaskType = TaskTypeMerge
		jsonData, _ := json.Marshal(map[string]any{b.column: delta})
		return b.tb.z.cacheAndPeriodicStore(ctx, b.tb.cacheKey, jsonData, task)
	default:
		return fmt.Errorf("UpdateColumn requires PeriodicCount or PeriodicMerge")
	}
}

// sqlBuilder 实现 SQLBuilder 接口
type sqlBuilder struct {
	z        *zmsg
	query    string
	args     []any
	cacheKey string
}

func (b *sqlBuilder) CacheKey(key string) SQLBuilder {
	b.cacheKey = key
	return b
}

func (b *sqlBuilder) Exec() (*SQLResult, error) {
	ctx := context.Background()
	task := &SQLTask{
		Query:  b.query,
		Params: b.args,
	}
	if b.cacheKey != "" {
		res, err := b.z.execSQL(ctx, task)
		if err == nil {
			_ = b.z.delCache(ctx, b.cacheKey)
		}
		return res, err
	}
	return b.z.execSQL(ctx, task)
}

func (b *sqlBuilder) QueryRow(dest ...any) error {
	ctx := context.Background()
	task := &SQLTask{
		Query:  b.query,
		Params: b.args,
	}

	// 智能判断：如果 dest 只有一个且是 Slice 指针，则执行 Query 并扫描列表
	if len(dest) == 1 {
		val := reflect.ValueOf(dest[0])
		if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Slice {
			rows, err := b.z.sqlExec.Query(ctx, convertSQLTask(task))
			if err != nil {
				return err
			}
			defer rows.Close()
			return scanSlice(rows, dest[0])
		}
	}

	return b.z.sqlExec.QueryRow(ctx, convertSQLTask(task)).Scan(dest...)
}

func (b *sqlBuilder) Query() (*sql.Rows, error) {
	ctx := context.Background()
	task := &SQLTask{
		Query:  b.query,
		Params: b.args,
	}
	return b.z.sqlExec.Query(ctx, convertSQLTask(task))
}

// ============ SQLTask 链式方法（PostgreSQL 特性）============

// WithType 设置任务类型
func (t *SQLTask) WithType(taskType TaskType) *SQLTask {
	t.TaskType = taskType
	return t
}

// WithBatchKey 设置批处理键
func (t *SQLTask) WithBatchKey(key string) *SQLTask {
	t.BatchKey = key
	return t
}

// OnConflict 添加 ON CONFLICT 子句
func (t *SQLTask) OnConflict(columns ...string) *SQLTask {
	t.Query += " ON CONFLICT (" + strings.Join(columns, ", ") + ")"
	return t
}

// DoUpdate 添加 DO UPDATE SET 子句
func (t *SQLTask) DoUpdate(columns ...string) *SQLTask {
	var sets []string
	for _, col := range columns {
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}
	t.Query += " DO UPDATE SET " + strings.Join(sets, ", ")
	return t
}

// DoUpdateSet 添加 DO UPDATE SET 子句（带自定义值）
func (t *SQLTask) DoUpdateSet(column string, value interface{}) *SQLTask {
	if t.paramIdx == 0 {
		t.paramIdx = len(t.Params) + 1
	}
	t.Query += fmt.Sprintf(" DO UPDATE SET %s = $%d", column, t.paramIdx)
	t.Params = append(t.Params, value)
	t.paramIdx++
	return t
}

// DoNothing 添加 DO NOTHING 子句
func (t *SQLTask) DoNothing() *SQLTask {
	t.Query += " DO NOTHING"
	return t
}

// Returning 添加 RETURNING 子句
func (t *SQLTask) Returning(columns ...string) *SQLTask {
	t.Query += " RETURNING "
	if len(columns) == 0 {
		t.Query += "*"
	} else {
		t.Query += strings.Join(columns, ", ")
	}
	return t
}

// convertPlaceholders 将 ? 转换为 PostgreSQL 的 $1, $2...
func convertPlaceholders(query string) string {
	var result strings.Builder
	paramIdx := 1

	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			result.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		} else {
			result.WriteByte(query[i])
		}
	}

	return result.String()
}

// scanSlice 扫描 Rows 到 Slice
func scanSlice(rows *sql.Rows, dest any) error {
	sliceVal := reflect.ValueOf(dest).Elem() // *[]Type -> []Type
	elemType := sliceVal.Type().Elem()       // Type

	// 确定元素类型（支持 struct 或 *struct）
	isPtr := false
	structType := elemType
	if elemType.Kind() == reflect.Ptr {
		isPtr = true
		structType = elemType.Elem()
	}

	if structType.Kind() != reflect.Struct {
		return fmt.Errorf("scanSlice: expected slice of struct or pointer to struct, got %s", elemType.Kind())
	}

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 预先计算列名到字段索引的映射
	fieldMap := make(map[string]int)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			name = field.Name // 默认使用字段名
		}
		fieldMap[name] = i
	}

	for rows.Next() {
		// 创建新元素
		newElem := reflect.New(structType).Elem() // Struct

		// 准备 Scan 目标
		scanDest := make([]any, len(columns))
		for i, col := range columns {
			if idx, ok := fieldMap[col]; ok {
				scanDest[i] = newElem.Field(idx).Addr().Interface()
			} else {
				// 忽略未知列
				var ignore any
				scanDest[i] = &ignore
			}
		}

		if err := rows.Scan(scanDest...); err != nil {
			return err
		}

		if isPtr {
			// 如果 slice 是 []*Struct，需要取地址
			sliceVal.Set(reflect.Append(sliceVal, newElem.Addr()))
		} else {
			// 如果 slice 是 []Struct，直接追加
			sliceVal.Set(reflect.Append(sliceVal, newElem))
		}
	}

	return rows.Err()
}
