package zmsg

import (
	"context"
	"net/http"
	"time"
)

// ZMsg 核心接口定义
type ZMsg interface {
	// Table 指定操作的表名
	Table(name string) TableBuilder

	// SQL 使用原生 SQL 进行操作
	SQL(query string, args ...any) SQLBuilder

	// 加载迁移相关（保持现状或后续重构）
	Load(filePath string) *Migration
	LoadSQL(sql string) *Migration
	LoadDir(dirPath string) *MigrationSet

	// NextID 生成全局唯一 ID
	NextID(ctx context.Context, prefix string) (string, error)

	// DBHit 检查缓存或布隆过滤器，判断数据是否可能在 DB 中
	DBHit(ctx context.Context, key string) bool

	// 指标
	MetricsHandler() http.Handler

	// 关闭
	Close() error
}

// TableBuilder 表操作构建器
type TableBuilder interface {
	// CacheKey 设置缓存键
	CacheKey(key string) TableBuilder

	// Serialize 设置分布式序列化键，确保相同 key 的操作按顺序执行
	Serialize(key string) TableBuilder

	// Where 设置 WHERE 条件（用于 background tasks 指定记录）
	Where(condition string, args ...any) TableBuilder

	// PeriodicOverride 周期性覆盖策略
	PeriodicOverride() TableBuilder

	// PeriodicMerge 周期性合并策略
	PeriodicMerge() TableBuilder

	// PeriodicCount 周期性计数策略
	PeriodicCount() TableBuilder

	// Save 保存完整行数据（INSERT ON CONFLICT DO UPDATE）
	Save(data any) error

	// UpdateColumns 更新指定列
	UpdateColumns(columns map[string]any) error

	// UpdateColumn 进入单列操作模式
	UpdateColumn() ColumnBuilder

	// Del 删除数据
	Del() error

	// Query 查询数据
	Query() ([]byte, error)
}

// ColumnBuilder 单列操作构建器
type ColumnBuilder interface {
	// Column 指定列名
	Column(name string) ColumnBuilder

	// Do 执行计算操作（如 zmsg.Add(), zmsg.Sub()）
	Do(fn ComputeFunc, delta any) error
}

// SQLBuilder 原生 SQL 构建器
type SQLBuilder interface {
	// CacheKey 设置缓存键（仅对查询/更新有效）
	CacheKey(key string) SQLBuilder

	// Exec 执行 SQL
	Exec() (*SQLResult, error)

	// QueryRow 执行 SQL 并扫描到 dest
	QueryRow(dest ...any) error
}

// ComputeFunc 周期性写计算函数
type ComputeFunc func(old any, delta any) any

// 计算函数实现（预定义）
func Add() ComputeFunc { return nil } // 内部标记占位
func Sub() ComputeFunc { return nil }
func Mul() ComputeFunc { return nil }

// SQLTask SQL 任务定义（内部保持对老逻辑的兼容或重构）
type SQLTask struct {
	Query    string        // SQL 语句
	Params   []interface{} // 参数
	TaskType TaskType      // 任务类型
	BatchKey string        // 批处理键（用于聚合）

	// 聚合相关字段
	Table     string        // 表名
	Column    string        // 列名
	Where     string        // WHERE 条件
	WhereArgs []interface{} // WHERE 参数

	// 兼容老逻辑的聚合字段
	OpType OpType      // 操作类型
	Delta  interface{} // 增量值

	// 内部字段
	SerializeKey string
	Strategy     PeriodicStrategy
	paramIdx     int
}

// OpType 操作类型
type OpType int

const (
	OpInc OpType = iota
	OpDec
	OpMul
	OpSet
	OpClean
	OpAdd
	OpDel
)

type PeriodicStrategy int

const (
	StrategyNormal PeriodicStrategy = iota
	StrategyOverride
	StrategyMerge
	StrategyCount
)

// TaskType 任务类型（用于周期写聚合策略）
type TaskType int

const (
	TaskTypeContent TaskType = iota
	TaskTypeCounter
	TaskTypeMerge // 新增：合并策略
)

// SQLResult SQL 执行结果
type SQLResult struct {
	LastInsertID int64
	RowsAffected int64
}

// Option 配置选项
type Option func(*Options)

// Options 选项结构
type Options struct {
	TTL         time.Duration
	Consistency Consistency
	AsyncDelay  time.Duration
}

// Consistency 一致性级别
type Consistency int

const (
	ConsistencyEventual Consistency = iota
	ConsistencyStrong
)
