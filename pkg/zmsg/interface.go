package zmsg

import (
	"context"
	"time"
)

// ZMsg 核心接口定义
type ZMsg interface {
	// 1. 仅缓存操作
	CacheOnly(ctx context.Context, key string, value []byte, opts ...Option) error

	// 2. 缓存并立即存储
	CacheAndStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask, opts ...Option) (string, error)

	// 3. 缓存并延迟存储
	CacheAndDelayStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask, opts ...Option) error

	// 4. 删除缓存
	Del(ctx context.Context, key string) error

	// 5. 删除并立即存储
	DelStore(ctx context.Context, key string, sqlTask *SQLTask) error

	// 6. 删除并延迟存储
	DelDelayStore(ctx context.Context, key string, sqlTask *SQLTask) error

	// 7. 更新缓存
	Update(ctx context.Context, key string, value []byte) error

	// 8. 更新并立即存储
	UpdateStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask) error

	// 9. 执行 SQL
	SQLExec(ctx context.Context, sqlTask *SQLTask) (*SQLResult, error)

	// 10. 检查 DB 命中（布隆过滤器）
	DBHit(ctx context.Context, key string) bool

	// 11. 生成下一个 ID
	NextID(ctx context.Context, prefix string) (string, error)

	// 查询
	Get(ctx context.Context, key string) ([]byte, error)

	// 批量操作
	Batch() BatchOperation

	// 关闭
	Close() error
}

// SQLTask SQL 任务定义
type SQLTask struct {
	Query    string        // SQL 语句
	Params   []interface{} // 参数
	TaskType TaskType      // 任务类型
	BatchKey string        // 批处理键（用于聚合）
}

// TaskType 任务类型
type TaskType int

const (
	TaskTypeContent  TaskType = iota // 内容存储
	TaskTypeCount                    // 计数器（支持批量聚合）
	TaskTypeRelation                 // 关系存储
)

// SQLResult SQL 执行结果
type SQLResult struct {
	LastInsertID int64
	RowsAffected int64
}

// BatchOperation 批处理操作
type BatchOperation interface {
	CacheOnly(key string, value []byte, opts ...Option)
	CacheAndStore(key string, value []byte, sqlTask *SQLTask, opts ...Option)
	CacheAndDelayStore(key string, value []byte, sqlTask *SQLTask, opts ...Option)
	Execute(ctx context.Context) error
	Reset()
	Size() int
}

// Option 配置选项
type Option func(*Options)

// Options 选项结构
type Options struct {
	TTL         time.Duration // 过期时间
	Consistency Consistency   // 一致性级别
	SyncPersist bool          // 是否同步持久化
	Priority    int           // 优先级
	Tags        []string      // 标签
}

// Consistency 一致性级别
type Consistency int

const (
	ConsistencyEventual Consistency = iota // 最终一致性
	ConsistencyStrong                      // 强一致性
)
