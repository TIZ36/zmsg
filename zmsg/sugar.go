package zmsg

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ============ 语法糖系统 ============
//
// 简约函数入口：
//
//	zmsg.Counter("feed_meta", "like_count").Inc(1).Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
//	zmsg.Slice("feed_meta", "tags").Add("tag1").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
//	zmsg.Map("feed_meta", "extra").Set("key", "val").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
//
// 链式调用风格（也支持）：
//
//	zmsg.Table("feed_meta").Column("like_count").Counter().Inc(1).Where(...).Build()
//
// 与 CacheAndPeriodicStore 配合使用时，会在内存中进行聚合：
//   - Counter: 多次 Inc(1) 会聚合为一次 +N
//   - Slice: 多次 Add 会聚合为一次批量追加
//   - Map: 多次 Set 会聚合为一次批量更新

// ============ 简约函数入口 ============

// Counter 创建计数器操作（简约入口）
//
// 示例:
//
//	zmsg.Counter("feed_meta", "like_count").Inc(1).Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
func Counter(table, column string) *CounterOp {
	return &CounterOp{
		table:    table,
		column:   column,
		paramIdx: 1,
	}
}

// Slice 创建数组操作（简约入口）
//
// 示例:
//
//	zmsg.Slice("feed_meta", "tags").Add("tag1").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
func Slice(table, column string) *SliceOp {
	return &SliceOp{
		table:    table,
		column:   column,
		paramIdx: 1,
	}
}

// Map 创建 Map 操作（简约入口）
//
// 示例:
//
//	zmsg.Map("feed_meta", "extra").Set("key", "val").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
func Map(table, column string) *MapOp {
	return &MapOp{
		table:    table,
		column:   column,
		paramIdx: 1,
	}
}

// ============ 链式调用入口（兼容） ============

// Table 创建表构建器（链式调用入口）
func Table(name string) *TableBuilder {
	return &TableBuilder{
		table: name,
	}
}

// TableBuilder 表构建器
type TableBuilder struct {
	table string
}

// Column 指定列
func (t *TableBuilder) Column(name string) *ColumnBuilder {
	return &ColumnBuilder{
		table:  t.table,
		column: name,
	}
}

// ColumnBuilder 列构建器
type ColumnBuilder struct {
	table  string
	column string
}

// Counter 返回计数器操作
func (c *ColumnBuilder) Counter() *CounterOp {
	return Counter(c.table, c.column)
}

// Slice 返回数组操作
func (c *ColumnBuilder) Slice() *SliceOp {
	return Slice(c.table, c.column)
}

// Map 返回 Map 操作
func (c *ColumnBuilder) Map() *MapOp {
	return Map(c.table, c.column)
}

// ============ CounterOp 计数器操作 ============

// CounterOp 计数器操作
type CounterOp struct {
	table     string
	column    string
	opType    OpType // 使用 OpType 代替 string
	value     interface{}
	where     string
	whereArgs []interface{}
	batchKey  string
	paramIdx  int
}

// Inc 增加
func (op *CounterOp) Inc(delta interface{}) *CounterOp {
	op.opType = OpInc
	op.value = delta
	return op
}

// Dec 减少
func (op *CounterOp) Dec(delta interface{}) *CounterOp {
	op.opType = OpDec
	op.value = delta
	return op
}

// Mul 乘以
func (op *CounterOp) Mul(factor interface{}) *CounterOp {
	op.opType = OpMul
	op.value = factor
	return op
}

// Set 直接设置值
func (op *CounterOp) Set(value interface{}) *CounterOp {
	op.opType = OpSet
	op.value = value
	return op
}

// Clean 清零
func (op *CounterOp) Clean() *CounterOp {
	op.opType = OpClean
	op.value = 0
	return op
}

// Get 获取当前值（返回 SELECT 查询，不参与聚合）
func (op *CounterOp) Get() *CounterOp {
	op.opType = OpType(-1) // 特殊标记，表示是 GET 操作
	return op
}

// Where 设置条件
func (op *CounterOp) Where(condition string, args ...interface{}) *CounterOp {
	op.where = condition
	op.whereArgs = args
	return op
}

// BatchKey 设置批处理键
func (op *CounterOp) BatchKey(key string) *CounterOp {
	op.batchKey = key
	return op
}

// Build 构建 SQLTask
func (op *CounterOp) Build() *SQLTask {
	var query strings.Builder
	var params []interface{}

	isGet := op.opType == OpType(-1)

	if isGet {
		// SELECT 查询
		query.WriteString(fmt.Sprintf("SELECT %s FROM %s", op.column, op.table))
	} else {
		switch op.opType {
		case OpInc:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = %s + $%d",
				op.table, op.column, op.column, op.paramIdx))
			params = append(params, op.value)
			op.paramIdx++
		case OpDec:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = %s - $%d",
				op.table, op.column, op.column, op.paramIdx))
			params = append(params, op.value)
			op.paramIdx++
		case OpMul:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = %s * $%d",
				op.table, op.column, op.column, op.paramIdx))
			params = append(params, op.value)
			op.paramIdx++
		case OpSet:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = $%d",
				op.table, op.column, op.paramIdx))
			params = append(params, op.value)
			op.paramIdx++
		case OpClean:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = 0", op.table, op.column))
		}
	}

	// 添加 WHERE 子句
	if op.where != "" {
		query.WriteString(" WHERE ")
		whereSQL := op.where
		for _, arg := range op.whereArgs {
			placeholder := fmt.Sprintf("$%d", op.paramIdx)
			op.paramIdx++
			whereSQL = strings.Replace(whereSQL, "?", placeholder, 1)
			params = append(params, arg)
		}
		query.WriteString(whereSQL)
	}

	taskType := TaskTypeCounter
	if isGet {
		taskType = TaskTypeContent
	}

	return &SQLTask{
		Query:     query.String(),
		Params:    params,
		TaskType:  taskType,
		BatchKey:  op.batchKey,
		Table:     op.table,
		Column:    op.column,
		Where:     op.where,
		WhereArgs: op.whereArgs,
		OpType:    op.opType,
		Delta:     op.value,
	}
}

// ============ SliceOp 数组操作 ============

// SliceOp 数组操作（JSONB 数组）
type SliceOp struct {
	table     string
	column    string
	opType    OpType // 使用 OpType
	value     interface{}
	where     string
	whereArgs []interface{}
	batchKey  string
	paramIdx  int
}

// Add 追加元素
func (op *SliceOp) Add(value interface{}) *SliceOp {
	op.opType = OpAdd
	op.value = value
	return op
}

// Del 删除元素
func (op *SliceOp) Del(value interface{}) *SliceOp {
	op.opType = OpDel
	op.value = value
	return op
}

// Clean 清空数组
func (op *SliceOp) Clean() *SliceOp {
	op.opType = OpClean
	return op
}

// Get 获取数组（不参与聚合）
func (op *SliceOp) Get() *SliceOp {
	op.opType = OpType(-1) // 特殊标记，表示是 GET 操作
	return op
}

// Where 设置条件
func (op *SliceOp) Where(condition string, args ...interface{}) *SliceOp {
	op.where = condition
	op.whereArgs = args
	return op
}

// BatchKey 设置批处理键
func (op *SliceOp) BatchKey(key string) *SliceOp {
	op.batchKey = key
	return op
}

// Build 构建 SQLTask
func (op *SliceOp) Build() *SQLTask {
	var query strings.Builder
	var params []interface{}

	isGet := op.opType == OpType(-1)

	if isGet {
		// SELECT 查询
		query.WriteString(fmt.Sprintf("SELECT %s FROM %s", op.column, op.table))
	} else {
		switch op.opType {
		case OpAdd:
			// JSONB 数组追加
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, '[]'::jsonb) || $%d::jsonb",
				op.table, op.column, op.column, op.paramIdx))
			jsonValue, _ := json.Marshal([]interface{}{op.value})
			params = append(params, string(jsonValue))
			op.paramIdx++
		case OpDel:
			// JSONB 数组删除
			query.WriteString(fmt.Sprintf(`UPDATE %s SET %s = (
				SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
				FROM jsonb_array_elements(%s) AS elem
				WHERE elem != $%d::jsonb
			)`, op.table, op.column, op.column, op.paramIdx))
			jsonValue, _ := json.Marshal(op.value)
			params = append(params, string(jsonValue))
			op.paramIdx++
		case OpClean:
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = '[]'::jsonb", op.table, op.column))
		}
	}

	// 添加 WHERE 子句
	if op.where != "" {
		query.WriteString(" WHERE ")
		whereSQL := op.where
		for _, arg := range op.whereArgs {
			placeholder := fmt.Sprintf("$%d", op.paramIdx)
			op.paramIdx++
			whereSQL = strings.Replace(whereSQL, "?", placeholder, 1)
			params = append(params, arg)
		}
		query.WriteString(whereSQL)
	}

	taskType := TaskTypeAppend
	if isGet {
		taskType = TaskTypeContent
	}

	return &SQLTask{
		Query:      query.String(),
		Params:     params,
		TaskType:   taskType,
		BatchKey:   op.batchKey,
		Table:      op.table,
		Column:     op.column,
		Where:      op.where,
		WhereArgs:  op.whereArgs,
		OpType:     op.opType,
		SliceValue: op.value,
	}
}

// ============ MapOp Map 操作 ============

// MapOp Map 操作（JSONB）
type MapOp struct {
	table     string
	column    string
	opType    OpType // 使用 OpType
	key       string
	value     interface{}
	where     string
	whereArgs []interface{}
	batchKey  string
	paramIdx  int
	isGetKey  bool // 是否是 GetKey 操作
}

// Set 设置键值对
func (op *MapOp) Set(key string, value interface{}) *MapOp {
	op.opType = OpSet
	op.key = key
	op.value = value
	return op
}

// Del 删除键
func (op *MapOp) Del(key string) *MapOp {
	op.opType = OpDel
	op.key = key
	return op
}

// Get 获取整个 Map（不参与聚合）
func (op *MapOp) Get() *MapOp {
	op.opType = OpType(-1) // 特殊标记
	return op
}

// GetKey 获取指定键的值（不参与聚合）
func (op *MapOp) GetKey(key string) *MapOp {
	op.opType = OpType(-1)
	op.key = key
	op.isGetKey = true
	return op
}

// Where 设置条件
func (op *MapOp) Where(condition string, args ...interface{}) *MapOp {
	op.where = condition
	op.whereArgs = args
	return op
}

// BatchKey 设置批处理键
func (op *MapOp) BatchKey(key string) *MapOp {
	op.batchKey = key
	return op
}

// Build 构建 SQLTask
func (op *MapOp) Build() *SQLTask {
	var query strings.Builder
	var params []interface{}

	isGet := op.opType == OpType(-1)

	if isGet {
		if op.isGetKey {
			// SELECT 特定键: column->>'key'
			query.WriteString(fmt.Sprintf("SELECT %s->>'%s' FROM %s", op.column, op.key, op.table))
		} else {
			// SELECT 整个 JSONB 列
			query.WriteString(fmt.Sprintf("SELECT %s FROM %s", op.column, op.table))
		}
	} else {
		switch op.opType {
		case OpSet:
			// JSONB 设置键值
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, '{}'::jsonb) || jsonb_build_object($%d, $%d::jsonb)",
				op.table, op.column, op.column, op.paramIdx, op.paramIdx+1))
			params = append(params, op.key)
			jsonValue, _ := json.Marshal(op.value)
			params = append(params, string(jsonValue))
			op.paramIdx += 2
		case OpDel:
			// JSONB 删除键
			query.WriteString(fmt.Sprintf("UPDATE %s SET %s = %s - $%d",
				op.table, op.column, op.column, op.paramIdx))
			params = append(params, op.key)
			op.paramIdx++
		}
	}

	// 添加 WHERE 子句
	if op.where != "" {
		query.WriteString(" WHERE ")
		whereSQL := op.where
		for _, arg := range op.whereArgs {
			placeholder := fmt.Sprintf("$%d", op.paramIdx)
			op.paramIdx++
			whereSQL = strings.Replace(whereSQL, "?", placeholder, 1)
			params = append(params, arg)
		}
		query.WriteString(whereSQL)
	}

	taskType := TaskTypePut
	if isGet {
		taskType = TaskTypeContent
	}

	return &SQLTask{
		Query:     query.String(),
		Params:    params,
		TaskType:  taskType,
		BatchKey:  op.batchKey,
		Table:     op.table,
		Column:    op.column,
		Where:     op.where,
		WhereArgs: op.whereArgs,
		OpType:    op.opType,
		MapKey:    op.key,
		MapValue:  op.value,
	}
}
