package zmsg

import (
	"fmt"
	"strings"
)

// SQL 创建原生 SQL 任务（? 自动转为 $1, $2...）
//
// 示例:
//
//	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content)
//	task := zmsg.SQL("UPDATE feeds SET content = ? WHERE id = ?", content, id)
//	task := zmsg.SQL("DELETE FROM feeds WHERE id = ?", id)
func SQL(query string, args ...interface{}) *SQLTask {
	// 将 ? 转换为 $1, $2, $3...
	converted := convertPlaceholders(query)
	return &SQLTask{
		Query:  converted,
		Params: args,
	}
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

// Builder SQL 构建器
type Builder struct {
	query    strings.Builder
	params   []interface{}
	paramIdx int
	taskType TaskType
	batchKey string
}

// NewBuilder 创建 SQL 构建器
func NewBuilder() *Builder {
	return &Builder{paramIdx: 1}
}

// Select 构建 SELECT 语句
func (b *Builder) Select(columns ...string) *Builder {
	b.query.WriteString("SELECT ")
	if len(columns) == 0 {
		b.query.WriteString("*")
	} else {
		b.query.WriteString(strings.Join(columns, ", "))
	}
	return b
}

// From 指定表名
func (b *Builder) From(table string) *Builder {
	b.query.WriteString(" FROM ")
	b.query.WriteString(table)
	return b
}

// Insert 构建 INSERT 语句
func (b *Builder) Insert(table string, data map[string]interface{}) *Builder {
	var columns []string
	var placeholders []string

	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, b.nextParam())
		b.params = append(b.params, val)
	}

	b.query.WriteString(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	))
	return b
}

// Update 构建 UPDATE 语句
func (b *Builder) Update(table string, data map[string]interface{}) *Builder {
	b.query.WriteString(fmt.Sprintf("UPDATE %s SET ", table))

	var sets []string
	for col, val := range data {
		sets = append(sets, fmt.Sprintf("%s = %s", col, b.nextParam()))
		b.params = append(b.params, val)
	}

	b.query.WriteString(strings.Join(sets, ", "))
	return b
}

// Delete 构建 DELETE 语句
func (b *Builder) Delete(table string) *Builder {
	b.query.WriteString(fmt.Sprintf("DELETE FROM %s", table))
	return b
}

// Where 添加 WHERE 条件
func (b *Builder) Where(condition string, value interface{}) *Builder {
	b.query.WriteString(" WHERE ")
	b.addCondition(condition, value)
	return b
}

// AndWhere 添加 AND 条件
func (b *Builder) AndWhere(condition string, value interface{}) *Builder {
	b.query.WriteString(" AND ")
	b.addCondition(condition, value)
	return b
}

// OrWhere 添加 OR 条件
func (b *Builder) OrWhere(condition string, value interface{}) *Builder {
	b.query.WriteString(" OR ")
	b.addCondition(condition, value)
	return b
}

// WhereIn 添加 IN 条件
func (b *Builder) WhereIn(column string, values []interface{}) *Builder {
	b.query.WriteString(" WHERE ")
	b.addInCondition(column, values)
	return b
}

// AndWhereIn 添加 AND IN 条件
func (b *Builder) AndWhereIn(column string, values []interface{}) *Builder {
	b.query.WriteString(" AND ")
	b.addInCondition(column, values)
	return b
}

// GroupBy 添加 GROUP BY
func (b *Builder) GroupBy(columns ...string) *Builder {
	b.query.WriteString(" GROUP BY ")
	b.query.WriteString(strings.Join(columns, ", "))
	return b
}

// OrderBy 添加 ORDER BY
func (b *Builder) OrderBy(column string, desc bool) *Builder {
	b.query.WriteString(" ORDER BY ")
	b.query.WriteString(column)
	if desc {
		b.query.WriteString(" DESC")
	}
	return b
}

// Limit 添加 LIMIT
func (b *Builder) Limit(limit int) *Builder {
	b.query.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return b
}

// Offset 添加 OFFSET
func (b *Builder) Offset(offset int) *Builder {
	b.query.WriteString(fmt.Sprintf(" OFFSET %d", offset))
	return b
}

// OnConflict 添加 ON CONFLICT 子句
func (b *Builder) OnConflict(columns ...string) *Builder {
	b.query.WriteString(" ON CONFLICT (")
	b.query.WriteString(strings.Join(columns, ", "))
	b.query.WriteString(")")
	return b
}

// DoUpdate 添加 DO UPDATE SET 子句（使用 EXCLUDED）
func (b *Builder) DoUpdate(columns ...string) *Builder {
	b.query.WriteString(" DO UPDATE SET ")

	var sets []string
	for _, col := range columns {
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}

	b.query.WriteString(strings.Join(sets, ", "))
	return b
}

// DoNothing 添加 DO NOTHING 子句
func (b *Builder) DoNothing() *Builder {
	b.query.WriteString(" DO NOTHING")
	return b
}

// Returning 添加 RETURNING 子句
func (b *Builder) Returning(columns ...string) *Builder {
	b.query.WriteString(" RETURNING ")
	if len(columns) == 0 {
		b.query.WriteString("*")
	} else {
		b.query.WriteString(strings.Join(columns, ", "))
	}
	return b
}

// TaskType 设置任务类型
func (b *Builder) TaskType(t TaskType) *Builder {
	b.taskType = t
	return b
}

// BatchKey 设置批处理键
func (b *Builder) BatchKey(key string) *Builder {
	b.batchKey = key
	return b
}

// Build 构建 SQLTask
func (b *Builder) Build() *SQLTask {
	return &SQLTask{
		Query:    b.query.String(),
		Params:   b.params,
		TaskType: b.taskType,
		BatchKey: b.batchKey,
	}
}

// Reset 重置构建器
func (b *Builder) Reset() *Builder {
	b.query.Reset()
	b.params = nil
	b.paramIdx = 1
	b.taskType = 0
	b.batchKey = ""
	return b
}

func (b *Builder) addCondition(condition string, value interface{}) {
	if strings.Contains(condition, "?") {
		parts := strings.Split(condition, "?")
		for i, part := range parts {
			b.query.WriteString(part)
			if i < len(parts)-1 {
				b.query.WriteString(b.nextParam())
				b.params = append(b.params, value)
			}
		}
	} else {
		b.query.WriteString(condition)
		if value != nil {
			b.query.WriteString(" ")
			b.query.WriteString(b.nextParam())
			b.params = append(b.params, value)
		}
	}
}

func (b *Builder) addInCondition(column string, values []interface{}) {
	b.query.WriteString(column)
	b.query.WriteString(" IN (")

	var placeholders []string
	for range values {
		placeholders = append(placeholders, b.nextParam())
	}

	b.query.WriteString(strings.Join(placeholders, ", "))
	b.query.WriteString(")")
	b.params = append(b.params, values...)
}

func (b *Builder) nextParam() string {
	p := fmt.Sprintf("$%d", b.paramIdx)
	b.paramIdx++
	return p
}
