package sql

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Task SQL任务
type Task struct {
	ID         string        `json:"id"`
	Query      string        `json:"query"`
	Params     []interface{} `json:"params,omitempty"`
	TaskType   TaskType      `json:"task_type"`
	BatchKey   string        `json:"batch_key,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
	Priority   int           `json:"priority,omitempty"`
	Timeout    time.Duration `json:"timeout,omitempty"`
	Retryable  bool          `json:"retryable,omitempty"`
	MaxRetries int           `json:"max_retries,omitempty"`
}

// TaskType 任务类型
type TaskType int

const (
	TaskTypeContent  TaskType = iota // 内容存储
	TaskTypeCount                    // 计数器（支持批量聚合）
	TaskTypeRelation                 // 关系存储
	TaskTypeSystem                   // 系统任务
)

// Result SQL执行结果
type Result struct {
	LastInsertID int64         `json:"last_insert_id,omitempty"`
	RowsAffected int64         `json:"rows_affected,omitempty"`
	Duration     time.Duration `json:"duration,omitempty"`
	Error        string        `json:"error,omitempty"`
}

// NewTask 创建SQL任务
func NewTask(query string, params ...interface{}) *Task {
	return &Task{
		ID:         generateTaskID(),
		Query:      query,
		Params:     params,
		TaskType:   TaskTypeContent,
		CreatedAt:  time.Now(),
		Retryable:  true,
		MaxRetries: 3,
	}
}

// NewInsertTask 创建插入任务
func NewInsertTask(table string, data map[string]interface{}) *Task {
	var columns []string
	var placeholders []string
	var params []interface{}

	i := 1
	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		params = append(params, val)
		i++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return NewTask(query, params...).WithType(TaskTypeContent)
}

// NewUpdateTask 创建更新任务
func NewUpdateTask(table string, data map[string]interface{}, where string, whereParams ...interface{}) *Task {
	var setClauses []string
	var params []interface{}

	i := 1
	for col, val := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, i))
		params = append(params, val)
		i++
	}

	// 添加WHERE条件参数
	params = append(params, whereParams...)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		table,
		strings.Join(setClauses, ", "),
		where,
	)

	return NewTask(query, params...).WithType(TaskTypeContent)
}

// NewDeleteTask 创建删除任务
func NewDeleteTask(table string, where string, params ...interface{}) *Task {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, where)
	return NewTask(query, params...).WithType(TaskTypeContent)
}

// NewSelectTask 创建查询任务
func NewSelectTask(table string, columns []string, where string, params ...interface{}) *Task {
	cols := "*"
	if len(columns) > 0 {
		cols = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s", cols, table)
	if where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}

	return NewTask(query, params...).WithType(TaskTypeContent)
}

// NewCounterTask 创建计数器任务
func NewCounterTask(table, column, where string, delta int64, whereParams ...interface{}) *Task {
	query := fmt.Sprintf(
		"UPDATE %s SET %s = %s + $1 WHERE %s",
		table, column, column, where,
	)

	params := make([]interface{}, 0, len(whereParams)+1)
	params = append(params, delta)
	params = append(params, whereParams...)

	return NewTask(query, params...).WithType(TaskTypeCount)
}

// WithType 设置任务类型
func (t *Task) WithType(taskType TaskType) *Task {
	t.TaskType = taskType
	return t
}

// WithBatchKey 设置批处理键
func (t *Task) WithBatchKey(batchKey string) *Task {
	t.BatchKey = batchKey
	return t
}

// WithPriority 设置优先级
func (t *Task) WithPriority(priority int) *Task {
	t.Priority = priority
	return t
}

// WithTimeout 设置超时时间
func (t *Task) WithTimeout(timeout time.Duration) *Task {
	t.Timeout = timeout
	return t
}

// WithRetryConfig 设置重试配置
func (t *Task) WithRetryConfig(retryable bool, maxRetries int) *Task {
	t.Retryable = retryable
	t.MaxRetries = maxRetries
	return t
}

// Validate 验证任务
func (t *Task) Validate() error {
	if t.Query == "" {
		return fmt.Errorf("query is required")
	}

	// 防止SQL注入的简单检查
	if strings.Contains(strings.ToUpper(t.Query), "DROP TABLE") {
		// 这里可以添加更复杂的SQL注入检查
		return fmt.Errorf("potentially dangerous SQL query")
	}

	return nil
}

// IsCounterTask 是否为计数器任务
func (t *Task) IsCounterTask() bool {
	return t.TaskType == TaskTypeCount
}

// IsBatchable 是否支持批处理
func (t *Task) IsBatchable() bool {
	return t.BatchKey != "" && (t.TaskType == TaskTypeCount || t.TaskType == TaskTypeRelation)
}

// Clone 克隆任务
func (t *Task) Clone() *Task {
	cloned := *t

	// 深拷贝Params
	if t.Params != nil {
		cloned.Params = make([]interface{}, len(t.Params))
		copy(cloned.Params, t.Params)
	}

	return &cloned
}

// generateTaskID 生成任务ID
func generateTaskID() string {
	return fmt.Sprintf("sql_%d_%d", time.Now().UnixNano(), rand.Int63())
}
