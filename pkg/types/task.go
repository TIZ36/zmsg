package sql

import (
	"fmt"
	"time"
)

// SQLTask SQL 任务
type SQLTask struct {
	Query     string
	Params    []interface{}
	TaskType  TaskType
	BatchKey  string
	CreatedAt time.Time
}

// TaskType 任务类型
type TaskType int

const (
	TaskTypeContent TaskType = iota
	TaskTypeCount
	TaskTypeRelation
)

// NewSQLTask 创建 SQL 任务
func NewSQLTask(query string, params ...interface{}) *SQLTask {
	return &SQLTask{
		Query:     query,
		Params:    params,
		TaskType:  TaskTypeContent,
		CreatedAt: time.Now(),
	}
}

// WithType 设置任务类型
func (t *SQLTask) WithType(taskType TaskType) *SQLTask {
	t.TaskType = taskType
	return t
}

// WithBatchKey 设置批处理键
func (t *SQLTask) WithBatchKey(batchKey string) *SQLTask {
	t.BatchKey = batchKey
	return t
}

// Validate 验证任务
func (t *SQLTask) Validate() error {
	if t.Query == "" {
		return fmt.Errorf("query is required")
	}
	return nil
}
