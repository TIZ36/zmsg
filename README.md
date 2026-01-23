# zmsg

<div align="center">

**高性能 Feed 存储引擎，面向高并发社交场景**

[![Go Version](https://img.shields.io/badge/go-1.24+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tiz36/zmsg?style=flat-square)](https://goreportcard.com/report/github.com/tiz36/zmsg)

[特性](#特性) • [快速开始](#快速开始) • [性能指标](#性能指标) • [架构设计](#架构设计) • [最佳实践](#最佳实践)

</div>

---

## 📖 简介

`zmsg` 是面向高并发社交场景的 Feed 存储引擎，通过**两级缓存**、**异步队列**和**内存聚合**三种写入策略，平衡一致性与吞吐量。

### 核心指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 读取延迟 | **171ns** | 并行场景，L1 缓存命中 |
| 写入吞吐 | **16K ops/sec** | 并行周期写入 |
| ID 生成 | **247ns** | 雪花算法 |
| 聚合效率 | **90%+** | 相同 BatchKey 的计数器操作合并为单次 DB 写入 |

### 核心特性

- **两级缓存**：L1 本地缓存（Ristretto）+ L2 Redis，读取自动逐级回源
- **三种写入模式**：同步写、延迟队列写（Asynq）、周期聚合写（BatchWriter）
- **内存聚合**：Counter/Slice/Map 操作按 BatchKey 聚合，周期批量落库
- **布隆过滤器**：快速判断 key 是否可能存在，减少无效 DB 查询
- **分布式 ID**：雪花算法 + PostgreSQL 节点自动分配
- **SQL 构建器**：链式 API，支持 ON CONFLICT、RETURNING

## 🚀 快速开始

### 安装

```bash
go get github.com/tiz36/zmsg
```

### 基本使用

```go
package main

import (
    "context"
    "github.com/tiz36/zmsg/zmsg"
)

func main() {
    ctx := context.Background()

    // 1. 初始化
    cfg, _ := zmsg.LoadConfig("config.yaml")
    zm, _ := zmsg.New(ctx, cfg)
    defer zm.Close()

    // 2. 数据库迁移
    zm.LoadDir("schema").Migrate(ctx)

    // 3. 生成 ID
    id, _ := zm.NextID(ctx, "feed")

    // 4. 写入（缓存 + DB）
    data := []byte(`{"content": "Hello"}`)
    task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, "Hello")
    zm.CacheAndStore(ctx, id, data, task)

    // 5. 读取（自动走缓存）
    result, _ := zm.Get(ctx, id)
}
```

## 📊 性能指标

基于实际 benchmark 测试（Apple M3 Pro, Go 1.24）：

### Benchmark 结果

| 操作 | 延迟 (ns/op) | 内存 (B/op) | 分配次数 |
|------|-------------|-------------|---------|
| **读取** |
| `Get` | 354.5 | 247 | 9 |
| `Get` (并行) | 171.6 | 240 | 9 |
| **写入** |
| `CacheOnly` | 369,877 | 1,017 | 30 |
| `CacheOnly` (并行) | 61,460 | 981 | 29 |
| `CacheAndPeriodicStore` | 336,532 | 2,847 | 51 |
| `CacheAndPeriodicStore` (并行) | 61,527 | 2,304 | 52 |
| **ID 生成** |
| `NextID` | 246.7 | 88 | 5 |
| `NextID` (并行) | 405.9 | 88 | 5 |
| **SQL 构建（纯 CPU）** |
| SQL Basic | 182.6 | 128 | 6 |
| SQL OnConflict | 330.1 | 384 | 12 |
| Counter Inc | 296.8 | 480 | 11 |
| Slice Add | 461.0 | 760 | 17 |
| Map Set | 475.5 | 848 | 17 |

> 运行 `make bench && make report` 生成完整报告和图表

## 🏗️ 架构设计

```
┌──────────────────────────────────────────────────────────────────────┐
│                            Application                                │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────┐
│                              zmsg                                     │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                         读取路径                                 │ │
│  │   Get(key) ──→ L1 Cache ──→ L2 Redis ──→ PostgreSQL            │ │
│  │              (Ristretto)    (miss时)      (miss时)              │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                         写入路径                                 │ │
│  │                                                                  │ │
│  │   CacheAndStore ────────→ L1/L2 Cache ──→ PostgreSQL (同步)    │ │
│  │                                                                  │ │
│  │   CacheAndDelayStore ───→ L1/L2 Cache ──→ Asynq Queue          │ │
│  │                                              │                   │ │
│  │                                              ▼                   │ │
│  │                                          PostgreSQL (异步)      │ │
│  │                                                                  │ │
│  │   CacheAndPeriodicStore → L1/L2 Cache ──→ BatchWriter          │ │
│  │                                           (内存聚合)            │ │
│  │                                              │                   │ │
│  │                                              ▼ (周期 flush)     │ │
│  │                                          PostgreSQL (批量)      │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────────┐│
│  │ Bloom Filter  │  │   ID Generator │  │     SQL Builder          ││
│  │ (穿透保护)    │  │  (Snowflake)   │  │ (Counter/Slice/Map)      ││
│  └───────────────┘  └───────────────┘  └───────────────────────────┘│
└──────────────────────────────────────────────────────────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   ┌─────────┐           ┌─────────┐           ┌───────────┐
   │  Redis  │           │  Redis  │           │ PostgreSQL│
   │ (Cache) │           │ (Asynq) │           │   (DB)    │
   └─────────┘           └─────────┘           └───────────┘
```

### 写入模式对比

| 模式 | 方法 | 一致性 | 吞吐量 | 适用场景 |
|------|------|--------|--------|---------|
| 同步写 | `CacheAndStore` | 强一致 | 低 | 订单、支付等关键数据 |
| 延迟写 | `CacheAndDelayStore` | 最终一致 | 中 | 评论、回复等可延迟数据 |
| 周期聚合 | `CacheAndPeriodicStore` | 最终一致 | 高 | 计数器、高频更新数据 |

### 聚合机制

`CacheAndPeriodicStore` 使用 BatchWriter 在内存中按 `BatchKey` 聚合同类操作：

- **Counter**：多次 `Inc(1)` 聚合为单次 `UPDATE SET col = col + N`
- **Slice**：多次 `Add()` 聚合为单次 JSONB 数组操作
- **Map**：多次 `Set()` 聚合为单次 JSONB 对象操作

聚合后按配置的 `batch_interval` 周期批量写入 PostgreSQL。

## 📚 API 文档

### 核心 API

| 方法 | 说明 | 写入策略 |
|------|------|---------|
| `CacheAndStore(ctx, key, data, task, opts...)` | 写缓存，同步执行 SQL | 同步写 DB |
| `CacheAndDelayStore(ctx, key, data, task, opts...)` | 写缓存，SQL 入 Asynq 队列 | 延迟写 DB |
| `CacheAndPeriodicStore(ctx, key, data, task)` | 写缓存，SQL 入 BatchWriter | 周期聚合写 DB |
| `CacheOnly(ctx, key, data, opts...)` | 仅写缓存 | 不写 DB |
| `Get(ctx, key)` | 读取，按 L1→L2→DB 顺序回源 | - |
| `Del(ctx, key)` | 删除缓存 | - |
| `DelStore(ctx, key, task)` | 删除缓存，执行 SQL | 同步写 DB |
| `SQLExec(ctx, task)` | 直接执行 SQL | 同步写 DB |
| `NextID(ctx, prefix)` | 生成分布式 ID（雪花算法） | - |
| `DBHit(ctx, key)` | 布隆过滤器判断 key 是否可能存在 | - |

### SQL 构建器

#### 原生 SQL（支持链式调用）

```go
// 基础用法
task := zmsg.SQL("UPDATE feeds SET content = ? WHERE id = ?", content, id)

// PostgreSQL ON CONFLICT
task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content).
    OnConflict("id").
    DoUpdate("content", "status")

task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content).
    OnConflict("id").
    DoNothing()

// RETURNING 子句
task := zmsg.SQL("INSERT INTO feeds (id) VALUES (?)", id).
    OnConflict("id").
    DoNothing().
    Returning("id", "created_at")
```

#### 语法糖（内存聚合 + 批量写入）

推荐使用链式调用风格，语义清晰：

```go
// Counter 计数器（推荐写法）
task := zmsg.Table("feed_reply_meta").Column("like_count").Counter().
    Inc(1).
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()

// Slice 数组（JSONB）
task := zmsg.Table("feed_reply_meta").Column("tags").Slice().
    Add("tag1").
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()

// Map 对象（JSONB）
task := zmsg.Table("feed_reply_meta").Column("extra").Map().
    Set("key", "val").
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()
```

也支持简约写法：

```go
zmsg.Counter("feed_meta", "like_count").Inc(1).Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
zmsg.Slice("feed_meta", "tags").Add("tag1").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
zmsg.Map("feed_meta", "extra").Set("k", "v").Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
```

#### 支持的操作

| 类型 | 操作 | 说明 |
|------|------|------|
| Counter | `Inc(n)` / `Dec(n)` / `Mul(n)` / `Set(n)` / `Clean()` | 计数器增减/乘/设置/清零 |
| Slice | `Add(val)` / `Del(val)` / `Clean()` | 数组追加/删除/清空 |
| Map | `Set(k, v)` / `Del(k)` | 对象设置/删除键 |

## ⚙️ 配置

```yaml
# 必需
postgres_dsn: "postgresql://user:pass@localhost/zmsg?sslmode=disable"
redis_addr: "localhost:6379"

# L1 缓存
l1_max_cost: 104857600  # 100MB

# 批量聚合
batch_interval: 5s
batch_size: 1000

# 缓存
default_ttl: 24h

# 队列（Asynq）
queue:
  addr: "localhost:6379"
  concurrency: 10
  task_delay: 1s
```

## 🎯 最佳实践

### 1. 根据一致性要求选择写入模式

```go
// 强一致性（订单、支付）：同步写
zm.CacheAndStore(ctx, orderID, orderData, task)

// 可延迟（评论、回复）：延迟队列写
zm.CacheAndDelayStore(ctx, replyID, replyData, task, zmsg.WithAsyncDelay(2*time.Second))

// 高频更新（点赞计数）：周期聚合写
task := zmsg.Counter("feed_meta", "like_count").Inc(1).Where("id = ?", feedID).BatchKey("meta:"+feedID).Build()
zm.CacheAndPeriodicStore(ctx, cacheKey, nil, task)
```

### 2. BatchKey 设计原则

BatchKey 决定哪些操作会被聚合：

```go
// ✅ 相同 BatchKey 的操作会聚合
// 500 次 Inc(1) 聚合为 1 次 UPDATE SET like_count = like_count + 500
for i := 0; i < 500; i++ {
    task := zmsg.Counter("feed_meta", "like_count").
        Inc(1).
        Where("id = ?", feedID).
        BatchKey("meta:" + feedID).  // 相同 BatchKey
        Build()
    zm.CacheAndPeriodicStore(ctx, fmt.Sprintf("like:%s:%d", feedID, i), nil, task)
}

// ❌ 不同 BatchKey 无法聚合
BatchKey("meta:" + feedID + ":" + time.Now().String())  // 每次都不同，无法聚合
```

### 3. 布隆过滤器使用

```go
// 先用布隆过滤器快速判断，再查缓存/DB
if !zm.DBHit(ctx, feedID) {
    // 布隆过滤器判断 key 肯定不存在
    return nil, ErrNotFound
}

// key 可能存在，继续查询
data, err := zm.Get(ctx, feedID)
```

### 4. 错误处理

```go
data, err := zm.Get(ctx, key)
if err != nil {
    if errors.Is(err, zmsg.ErrNotFound) {
        return nil, fmt.Errorf("feed not found: %s", key)
    }
    return nil, fmt.Errorf("get feed failed: %w", err)
}
```

## 🔍 适用场景

### ✅ 适用

| 场景 | 原因 |
|------|------|
| Feed 流读取 | L1 缓存命中时延迟 171ns |
| 点赞/关注计数 | Counter 聚合减少 DB 写入 |
| 高并发写入 | 三种写入模式灵活选择 |

### ⚠️ 需评估

| 场景 | 建议 |
|------|------|
| 强一致性要求 | 使用 `CacheAndStore` 同步写 |
| 复杂关系查询 | zmsg 不提供 ORM，考虑配合 Ent/GORM |

### ❌ 不适用

| 场景 | 替代方案 |
|------|---------|
| OLAP 分析 | ClickHouse、BigQuery |
| 简单 CRUD | GORM、Ent |
| 无 Redis 环境 | 直接使用 PostgreSQL |

## 🛠️ 开发

```bash
# 启动依赖
docker-compose up -d

# 运行测试
make test

# 运行 benchmark
make bench

# 生成性能报告（Markdown + SVG 图表）
make report

# 生成行业分析报告
make analyse
```

## 📦 部署

### 依赖

- PostgreSQL 12+
- Redis 6+

### 配置调优

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `l1_max_cost` | 100MB | L1 缓存大小，根据内存调整 |
| `batch_interval` | 5s | 聚合写入周期，越短延迟越低 |
| `batch_size` | 1000 | 单次批量写入上限 |
| `default_ttl` | 24h | 缓存默认过期时间 |

## 📄 License

MIT

## 🙏 依赖

- [Ristretto](https://github.com/dgraph-io/ristretto) - L1 本地缓存
- [Asynq](https://github.com/hibiken/asynq) - Redis 任务队列
- [Bloom](https://github.com/bits-and-blooms/bloom) - 布隆过滤器