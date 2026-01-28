package zmsg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
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

func (b *tableBuilder) Query(dest any) error {
	if dest == nil {
		return fmt.Errorf("Query dest is nil")
	}
	ctx := context.Background()
	if b.cacheKey != "" {
		if data, err := b.z.cacheGet(ctx, b.cacheKey); err == nil && len(data) > 0 {
			if err := json.Unmarshal(data, dest); err != nil {
				return err
			}
			return nil
		} else if err != nil && err != ErrNotFound {
			return err
		}
	}

	query, args, err := b.buildSelectQuery(dest)
	if err != nil {
		return err
	}
	rows, err := b.z.sqlExec.Query(ctx, convertSQLTask(&SQLTask{Query: query, Params: args}))
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := scanStruct(rows, dest); err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	if b.cacheKey != "" {
		if data, err := json.Marshal(dest); err == nil {
			_ = b.z.updateCache(ctx, b.cacheKey, data, b.z.config.DefaultTTL)
		}
	}
	return nil
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

func (b *tableBuilder) buildSelectQuery(dest any) (string, []any, error) {
	if b.table == "" {
		return "", nil, fmt.Errorf("table name is required")
	}
	v := reflect.Indirect(reflect.ValueOf(dest))
	if v.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("Query dest must be a struct or pointer to struct")
	}
	cols, pkCols := structColumnsAndPK(v.Type())

	selectCols := "*"
	if len(cols) > 0 {
		selectCols = strings.Join(cols, ", ")
	}

	where := b.where
	whereArgs := b.whereArgs
	if where == "" && b.cacheKey != "" {
		pk := "id"
		if len(pkCols) == 1 {
			pk = pkCols[0]
		} else if len(pkCols) > 1 {
			return "", nil, fmt.Errorf("composite primary key requires Where()")
		}
		where = fmt.Sprintf("%s = ?", pk)
		whereArgs = []any{b.cacheKey}
	}
	if where == "" {
		return "", nil, fmt.Errorf("Query requires CacheKey() or Where()")
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", selectCols, b.table, where)
	return convertPlaceholders(query), whereArgs, nil
}

func structColumnsAndPK(t reflect.Type) ([]string, []string) {
	var cols []string
	var pkCols []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		parts := strings.Split(tag, ",")
		colName := parts[0]
		if colName == "" {
			colName = field.Name
		}
		cols = append(cols, colName)
		for _, p := range parts[1:] {
			if p == "pk" {
				pkCols = append(pkCols, colName)
				break
			}
		}
	}
	return cols, pkCols
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

func (b *sqlBuilder) Query(dest ...any) error {
	if len(dest) == 0 {
		return fmt.Errorf("Query requires at least one dest")
	}
	ctx := context.Background()
	task := &SQLTask{
		Query:  b.query,
		Params: b.args,
	}

	if len(dest) == 1 && b.cacheKey != "" {
		if data, err := b.z.cacheGet(ctx, b.cacheKey); err == nil && len(data) > 0 {
			if err := json.Unmarshal(data, dest[0]); err != nil {
				return err
			}
			return nil
		} else if err != nil && err != ErrNotFound {
			return err
		}
	}

	if len(dest) == 1 {
		val := reflect.ValueOf(dest[0])
		if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Slice {
			rows, err := b.z.sqlExec.Query(ctx, convertSQLTask(task))
			if err != nil {
				return err
			}
			defer rows.Close()
			if err := scanSlice(rows, dest[0]); err != nil {
				return err
			}
		} else if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct {
			rows, err := b.z.sqlExec.Query(ctx, convertSQLTask(task))
			if err != nil {
				return err
			}
			defer rows.Close()
			if err := scanStruct(rows, dest[0]); err != nil {
				if err == sql.ErrNoRows {
					return ErrNotFound
				}
				return err
			}
		} else if val.Kind() == reflect.Ptr {
			if err := b.z.sqlExec.QueryRowScan(ctx, convertSQLTask(task), dest...); err != nil {
				if err == sql.ErrNoRows {
					return ErrNotFound
				}
				return err
			}
		} else {
			return fmt.Errorf("Query dest must be pointer")
		}
	} else {
		if err := b.z.sqlExec.QueryRowScan(ctx, convertSQLTask(task), dest...); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
	}

	if len(dest) == 1 && b.cacheKey != "" {
		val := reflect.ValueOf(dest[0])
		if val.Kind() == reflect.Ptr && (val.Elem().Kind() == reflect.Struct || val.Elem().Kind() == reflect.Slice) {
			if data, err := json.Marshal(dest[0]); err == nil {
				_ = b.z.updateCache(ctx, b.cacheKey, data, b.z.config.DefaultTTL)
			}
		}
	}

	return nil
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
		postScan := make([]func(), 0, len(columns))
		for i, col := range columns {
			if idx, ok := fieldMap[col]; ok {
				field := newElem.Field(idx)
				scanTarget, apply := buildScanTarget(field)
				scanDest[i] = scanTarget
				if apply != nil {
					postScan = append(postScan, apply)
				}
			} else {
				// 忽略未知列
				var ignore any
				scanDest[i] = &ignore
			}
		}

		if err := rows.Scan(scanDest...); err != nil {
			return err
		}
		for _, apply := range postScan {
			apply()
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

// scanStruct 扫描 Rows 到单个 Struct（仅取第一行）
func scanStruct(rows *sql.Rows, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("scanStruct: expected pointer to struct, got %s", val.Kind())
	}

	structVal := val.Elem()
	structType := structVal.Type()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fieldMap := make(map[string]int)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			name = field.Name
		}
		fieldMap[name] = i
	}

	if !rows.Next() {
		return sql.ErrNoRows
	}

	scanDest := make([]any, len(columns))
	postScan := make([]func(), 0, len(columns))
	for i, col := range columns {
		if idx, ok := fieldMap[col]; ok {
			field := structVal.Field(idx)
			scanTarget, apply := buildScanTarget(field)
			scanDest[i] = scanTarget
			if apply != nil {
				postScan = append(postScan, apply)
			}
		} else {
			var ignore any
			scanDest[i] = &ignore
		}
	}

	if err := rows.Scan(scanDest...); err != nil {
		return err
	}
	for _, apply := range postScan {
		apply()
	}

	return rows.Err()
}

// scanScalar 扫描 Rows 到单个基础类型（仅取第一行）
func scanScalar(rows *sql.Rows, dest any) error {
	if !rows.Next() {
		return sql.ErrNoRows
	}

	scanDest, apply, err := buildScanTargetForPtr(dest)
	if err != nil {
		return err
	}

	if err := rows.Scan(scanDest); err != nil {
		return err
	}
	if apply != nil {
		apply()
	}
	return rows.Err()
}

var (
	nullStringType  = reflect.TypeOf(sql.NullString{})
	nullInt64Type   = reflect.TypeOf(sql.NullInt64{})
	nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
	nullBoolType    = reflect.TypeOf(sql.NullBool{})
	nullTimeType    = reflect.TypeOf(sql.NullTime{})
	timeType        = reflect.TypeOf(time.Time{})
)

func buildScanTarget(field reflect.Value) (any, func()) {
	if !field.CanSet() {
		var ignore any
		return &ignore, nil
	}

	fieldType := field.Type()
	switch fieldType {
	case nullStringType, nullInt64Type, nullFloat64Type, nullBoolType, nullTimeType:
		return field.Addr().Interface(), nil
	}

	// 指针字段：NULL -> nil，非 NULL -> 指向具体值
	if field.Kind() == reflect.Ptr {
		elem := fieldType.Elem()
		switch elem.Kind() {
		case reflect.String:
			ns := &sql.NullString{}
			return ns, func() {
				if ns.Valid {
					v := ns.String
					field.Set(reflect.ValueOf(&v))
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ni := &int64Scanner{}
			return ni, func() {
				if ni.Valid {
					v := reflect.New(elem).Elem()
				v.SetInt(ni.Int64)
					field.Set(v.Addr())
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		nu := &int64Scanner{}
			return nu, func() {
				if nu.Valid && nu.Int64 >= 0 {
					v := reflect.New(elem).Elem()
				v.SetUint(uint64(nu.Int64))
					field.Set(v.Addr())
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		case reflect.Float32, reflect.Float64:
			nf := &sql.NullFloat64{}
			return nf, func() {
				if nf.Valid {
					v := reflect.New(elem).Elem()
					v.SetFloat(nf.Float64)
					field.Set(v.Addr())
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		case reflect.Bool:
			nb := &sql.NullBool{}
			return nb, func() {
				if nb.Valid {
					v := reflect.New(elem).Elem()
					v.SetBool(nb.Bool)
					field.Set(v.Addr())
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		default:
			if elem == timeType {
				nt := &sql.NullTime{}
				return nt, func() {
					if nt.Valid {
						v := reflect.New(elem).Elem()
						v.Set(reflect.ValueOf(nt.Time))
						field.Set(v.Addr())
					} else {
						field.Set(reflect.Zero(fieldType))
					}
				}
			}
		}
	}

	// 非指针字段：NULL -> 零值
	switch field.Kind() {
	case reflect.String:
		ns := &sql.NullString{}
		return ns, func() {
			if ns.Valid {
				field.SetString(ns.String)
			} else {
				field.SetString("")
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	ni := &int64Scanner{}
		return ni, func() {
			if ni.Valid {
				field.SetInt(ni.Int64)
			} else {
				field.SetInt(0)
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	nu := &int64Scanner{}
		return nu, func() {
			if nu.Valid && nu.Int64 >= 0 {
				field.SetUint(uint64(nu.Int64))
			} else {
				field.SetUint(0)
			}
		}
	case reflect.Float32, reflect.Float64:
		nf := &sql.NullFloat64{}
		return nf, func() {
			if nf.Valid {
				field.SetFloat(nf.Float64)
			} else {
				field.SetFloat(0)
			}
		}
	case reflect.Bool:
		nb := &sql.NullBool{}
		return nb, func() {
			if nb.Valid {
				field.SetBool(nb.Bool)
			} else {
				field.SetBool(false)
			}
		}
	default:
		if fieldType == timeType {
			nt := &sql.NullTime{}
			return nt, func() {
				if nt.Valid {
					field.Set(reflect.ValueOf(nt.Time))
				} else {
					field.Set(reflect.Zero(fieldType))
				}
			}
		}
	}

	return field.Addr().Interface(), nil
}

func buildScanTargetsForPtrs(dest []any) ([]any, []func(), error) {
	scanDest := make([]any, len(dest))
	postScan := make([]func(), 0, len(dest))
	for i, d := range dest {
		target, apply, err := buildScanTargetForPtr(d)
		if err != nil {
			return nil, nil, err
		}
		scanDest[i] = target
		if apply != nil {
			postScan = append(postScan, apply)
		}
	}
	return scanDest, postScan, nil
}

func buildScanTargetForPtr(dest any) (any, func(), error) {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return nil, nil, fmt.Errorf("scan target must be pointer, got %s", val.Kind())
	}

	elem := val.Elem()
	elemType := elem.Type()
	switch elemType {
	case nullStringType, nullInt64Type, nullFloat64Type, nullBoolType, nullTimeType:
		return dest, nil, nil
	}

	switch elem.Kind() {
	case reflect.String:
		ns := &sql.NullString{}
		return ns, func() {
			if ns.Valid {
				elem.SetString(ns.String)
			} else {
				elem.SetString("")
			}
		}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	ni := &int64Scanner{}
		return ni, func() {
			if ni.Valid {
				elem.SetInt(ni.Int64)
			} else {
				elem.SetInt(0)
			}
		}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	nu := &int64Scanner{}
		return nu, func() {
			if nu.Valid && nu.Int64 >= 0 {
				elem.SetUint(uint64(nu.Int64))
			} else {
				elem.SetUint(0)
			}
		}, nil
	case reflect.Float32, reflect.Float64:
		nf := &sql.NullFloat64{}
		return nf, func() {
			if nf.Valid {
				elem.SetFloat(nf.Float64)
			} else {
				elem.SetFloat(0)
			}
		}, nil
	case reflect.Bool:
		nb := &sql.NullBool{}
		return nb, func() {
			if nb.Valid {
				elem.SetBool(nb.Bool)
			} else {
				elem.SetBool(false)
			}
		}, nil
	default:
		if elemType == timeType {
			nt := &sql.NullTime{}
			return nt, func() {
				if nt.Valid {
					elem.Set(reflect.ValueOf(nt.Time))
				} else {
					elem.Set(reflect.Zero(elemType))
				}
			}, nil
		}
	}

	return dest, nil, nil
}

type int64Scanner struct {
	Valid bool
	Int64 int64
}

func (s *int64Scanner) Scan(src any) error {
	if src == nil {
		s.Valid = false
		s.Int64 = 0
		return nil
	}

	switch v := src.(type) {
	case int64:
		s.Valid = true
		s.Int64 = v
		return nil
	case int32:
		s.Valid = true
		s.Int64 = int64(v)
		return nil
	case int:
		s.Valid = true
		s.Int64 = int64(v)
		return nil
	case float64:
		s.Valid = true
		s.Int64 = int64(v)
		return nil
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return err
		}
		s.Valid = true
		s.Int64 = n
		return nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		s.Valid = true
		s.Int64 = n
		return nil
	case time.Time:
		s.Valid = true
		s.Int64 = v.Unix()
		return nil
	default:
		return fmt.Errorf("unsupported int64 scan type %T", src)
	}
}
