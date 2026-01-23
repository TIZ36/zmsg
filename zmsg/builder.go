package zmsg

import (
	"fmt"
	"strings"
)

// SQL 创建原生 SQL 任务（? 自动转为 $1, $2...）
//
// 基础用法（直接使用）:
//
//	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content)
//	task := zmsg.SQL("UPDATE feeds SET content = ? WHERE id = ?", content, id)
//
// 链式调用（PostgreSQL 特性）:
//
//	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content).
//	    OnConflict("id").
//	    DoUpdate("content", "status")
//
//	task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content).
//	    OnConflict("id").
//	    DoNothing()
func SQL(query string, args ...interface{}) *SQLTask {
	// 将 ? 转换为 $1, $2, $3...
	converted := convertPlaceholders(query)
	return &SQLTask{
		Query:    converted,
		Params:   args,
		paramIdx: len(args) + 1,
	}
}

// ============ SQLTask 链式方法（PostgreSQL 特性）============

// OnConflict 添加 ON CONFLICT 子句
//
// 示例: .OnConflict("id") -> ON CONFLICT (id)
// 示例: .OnConflict("id", "user_id") -> ON CONFLICT (id, user_id)
func (t *SQLTask) OnConflict(columns ...string) *SQLTask {
	t.Query += " ON CONFLICT (" + strings.Join(columns, ", ") + ")"
	return t
}

// DoUpdate 添加 DO UPDATE SET 子句（使用 EXCLUDED）
//
// 示例: .DoUpdate("content", "status") -> DO UPDATE SET content = EXCLUDED.content, status = EXCLUDED.status
func (t *SQLTask) DoUpdate(columns ...string) *SQLTask {
	var sets []string
	for _, col := range columns {
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}
	t.Query += " DO UPDATE SET " + strings.Join(sets, ", ")
	return t
}

// DoUpdateSet 添加 DO UPDATE SET 子句（带自定义值）
//
// 示例: .DoUpdateSet("status", "active") -> DO UPDATE SET status = $N
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
//
// 示例: .Returning("id", "created_at") -> RETURNING id, created_at
// 示例: .Returning() -> RETURNING *
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
