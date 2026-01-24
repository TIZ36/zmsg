package zmsg

import (
	"context"
	"net/http"
	"time"
)

// ZMsg 核心接口定义
type ZMsg interface {
	// 1. 仅缓存操作
	CacheOnly(ctx context.Context, key string, value []byte, opts ...Option) error

	// 2. 缓存并立即存储（强一致性）
	CacheAndStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask, opts ...Option) (string, error)

	// 3. 缓存并延迟存储（asynq 延迟队列，通过 WithAsyncDelay 设置延迟）
	CacheAndDelayStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask, opts ...Option) error

	// 4. 缓存并周期存储（内存聚合 + 定时 flush，支持 Counter/Content 类型）
	CacheAndPeriodicStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask, opts ...Option) error

	// 5. 删除缓存
	Del(ctx context.Context, key string) error

	// 6. 删除并立即存储
	DelStore(ctx context.Context, key string, sqlTask *SQLTask) error

	// 7. 删除并延迟存储
	DelDelayStore(ctx context.Context, key string, sqlTask *SQLTask) error

	// 8. 更新缓存
	Update(ctx context.Context, key string, value []byte) error

	// 9. 更新并立即存储
	UpdateStore(ctx context.Context, key string, value []byte, sqlTask *SQLTask) error

	// 10. 执行 SQL
	SQLExec(ctx context.Context, sqlTask *SQLTask) (*SQLResult, error)

	// 11. 检查 DB 命中（布隆过滤器）
	DBHit(ctx context.Context, key string) bool

	// 12. 生成下一个 ID
	NextID(ctx context.Context, prefix string) (string, error)

	// 查询
	Get(ctx context.Context, key string) ([]byte, error)

	// 批量操作
	Batch() BatchOperation

	// 数据库迁移
	Load(filePath string) *Migration
	LoadSQL(sql string) *Migration
	LoadDir(dirPath string) *MigrationSet

	// MetricsHandler 返回指标 HTTP 处理器
	MetricsHandler() http.Handler

	// 关闭
	Close() error
}

// OpType 操作类型（用于聚合）
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

// SQLTask SQL 任务定义
type SQLTask struct {
	Query    string        // SQL 语句
	Params   []interface{} // 参数
	TaskType TaskType      // 任务类型
	BatchKey string        // 批处理键（用于聚合）

	// 聚合相关字段
	Table     string        // 表名
	Column    string        // 列名
	Where     string        // WHERE 条件（不含 WHERE 关键字）
	WhereArgs []interface{} // WHERE 参数

	// Counter 聚合
	OpType OpType      // 操作类型
	Delta  interface{} // 增量值

	// Slice 聚合
	SliceValue interface{} // 要追加或删除的值

	// Map 聚合
	MapKey   string      // Map 键
	MapValue interface{} // Map 值

	// 内部字段（链式调用用）
	paramIdx int // 下一个参数占位符索引
}

// WithType 设置任务类型（链式调用）
func (t *SQLTask) WithType(taskType TaskType) *SQLTask {
	t.TaskType = taskType
	return t
}

// WithBatchKey 设置批处理键（链式调用）
func (t *SQLTask) WithBatchKey(key string) *SQLTask {
	t.BatchKey = key
	return t
}

// TaskType 任务类型（用于周期写聚合策略）
type TaskType int

const (
	// TaskTypeContent 内容存储（相同 key 覆盖，只执行最新的 task）
	TaskTypeContent TaskType = iota

	// TaskTypeCounter 计数器（支持累加聚合）
	TaskTypeCounter

	// TaskTypeAppend Slice 追加（聚合所有 task）
	TaskTypeAppend

	// TaskTypePut Map 写入（聚合所有 task）
	TaskTypePut
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
	AsyncDelay  time.Duration // asynq 任务延迟时间
}

// Consistency 一致性级别
type Consistency int

const (
	ConsistencyEventual Consistency = iota // 最终一致性
	ConsistencyStrong                      // 强一致性
)
