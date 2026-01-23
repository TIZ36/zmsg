# zmsg

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**高性能消息/Feed 存储引擎**，专为大规模社交场景设计。

## 核心特性

- **多级缓存架构** — L1 本地缓存 (Ristretto) + L2 分布式缓存 (Redis)，毫秒级读取
- **智能穿透保护** — 布隆过滤器 + SingleFlight，有效防止缓存穿透和击穿
- **三种写入策略** — 同步写入 / Asynq 延迟队列 / 内存聚合周期写入，灵活平衡一致性与吞吐量
- **内存聚合引擎** — Counter/Slice/Map 操作自动聚合，大幅减少 DB 写入次数
- **分布式 ID** — 雪花算法 + PostgreSQL 节点自动分配，支持多实例部署
- **SQL 迁移** — 增量迁移，自动跳过已执行

## 架构设计

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                               Application                                       │
└────────────────────────────────────┬───────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────────────┐
│                                  zmsg                                           │
│                                                                                 │
│  ┌─────────────────────────────── READ PATH ──────────────────────────────────┐│
│  │                                                                             ││
│  │   ┌─────────┐    ┌─────────────┐    ┌─────────┐    ┌─────────┐            ││
│  │   │  Bloom  │ →  │ L1 (Local)  │ →  │SingleFlt│ →  │L2(Redis)│ → DB       ││
│  │   │ Filter  │    │  Ristretto  │    │  防击穿  │    │         │            ││
│  │   └─────────┘    └─────────────┘    └─────────┘    └─────────┘            ││
│  │    缓存穿透          本地缓存           并发控制        分布式缓存    回源查询  ││
│  │                                                                             ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  ┌────────────────────────────── WRITE PATH ──────────────────────────────────┐│
│  │                                                                             ││
│  │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐││
│  │   │  CacheAndStore  │  │CacheAndDelayStore│  │  CacheAndPeriodicStore     │││
│  │   │    同步写入      │  │   异步队列写入   │  │       周期聚合写入          │││
│  │   │                 │  │                 │  │                             │││
│  │   │  Cache → DB     │  │  Cache → Asynq  │  │  Cache → PeriodicWriter     │││
│  │   │  (强一致)        │  │  → Worker → DB  │  │  → Memory Aggregation      │││
│  │   │                 │  │  (最终一致)      │  │  → Batch Flush → DB        │││
│  │   └────────┬────────┘  └────────┬────────┘  └──────────────┬──────────────┘││
│  │            │                    │                          │               ││
│  │            │         ┌──────────▼──────────┐    ┌──────────▼──────────┐   ││
│  │            │         │   Asynq Queue       │    │  PeriodicWriter     │   ││
│  │            │         │ (Redis-based)       │    │  ┌───────────────┐  │   ││
│  │            │         │ ┌───────────────┐   │    │  │ ShardedBuffer │  │   ││
│  │            │         │ │ TypeSave      │   │    │  │ (16 shards)   │  │   ││
│  │            │         │ │ TypeDelete    │   │    │  ├───────────────┤  │   ││
│  │            │         │ │ TypeUpdate    │   │    │  │Counter 聚合    │  │   ││
│  │            │         │ │ TypeCacheRepair│  │    │  │Slice   聚合    │  │   ││
│  │            │         │ └───────────────┘   │    │  │Map     聚合    │  │   ││
│  │            │         └─────────┬───────────┘    │  │Content 覆盖    │  │   ││
│  │            │                   │                │  └───────────────┘  │   ││
│  │            │                   │                │  触发: 周期/队列长度 │   ││
│  │            │                   │                └──────────┬──────────┘   ││
│  │            │                   │                           │               ││
│  └────────────┼───────────────────┼───────────────────────────┼───────────────┘│
│               │                   │                           │                │
│  ┌────────────▼───────────────────▼───────────────────────────▼───────────────┐│
│  │                           PostgreSQL                                        ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  ┌─────────────────────────────── ID GEN ─────────────────────────────────────┐│
│  │   Snowflake (41bit 时间 + 10bit 节点 + 12bit 序列)                          ││
│  │   PostgreSQL 节点自动分配 + 心跳续约                                         ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────────────────────────────────┘
```

## 性能基准

> 测试环境：MacBook Pro M1, PostgreSQL 15, Redis 7

### 核心操作延迟

| 操作 | 单线程 | 并行 (GOMAXPROCS=8) | 内存分配 |
|------|--------|---------------------|----------|
| **Get (缓存命中)** | 351ns | **166ns** | 248B/9allocs |
| **CacheOnly** | 306.86µs | **60.54µs** | 1KB/30allocs |
| **CacheAndPeriodicStore** | 313.73µs | **60.85µs** | 2.3KB/52allocs |
| **NextID** | 268ns | 388ns | 88B/5allocs |

### SQL Builder 性能

| 操作 | 吞吐量 | 延迟 | 内存分配 |
|------|--------|------|----------|
| SQL_Basic | 11.6M ops/s | 179ns | 128B/6allocs |
| SQL_OnConflict | 6.5M ops/s | 336ns | 384B/12allocs |
| Counter_Inc | 7.7M ops/s | 286ns | 480B/11allocs |
| Slice_Add | 4.4M ops/s | 463ns | 760B/17allocs |
| Map_Set | 4.9M ops/s | 481ns | 848B/17allocs |

### 性能指标图

![Benchmark Results](tests/reports/latest.svg)

## 适用场景与规模

| 场景 | 推荐配置 | 预估 QPS |
|------|----------|----------|
| **小型项目** (DAU < 10万) | 单节点, 1GB L1, 4GB Redis | 10K+ 读 / 5K+ 写 |
| **中型项目** (DAU 10-100万) | 2-4节点, 4GB L1, 16GB Redis | 50K+ 读 / 20K+ 写 |
| **大型项目** (DAU > 100万) | 8+节点, 16GB L1, 64GB Redis Cluster | 200K+ 读 / 100K+ 写 |

**典型使用场景**：
- 社交 Feed 流（发布、点赞、评论）
- 消息系统（IM 消息存储、已读状态）
- 计数服务（关注数、粉丝数、浏览量）
- 用户状态（在线状态、活跃度）

## 安装

```bash
go get github.com/tiz36/zmsg
```

**依赖服务**：
- PostgreSQL 12+
- Redis 6+

## 快速开始

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

    // 3. 生成分布式 ID
    id, _ := zm.NextID(ctx, "feed")

    // 4. 写入（三种策略）
    data := []byte(`{"content": "Hello World"}`)
    
    // 4.1 同步写入（强一致）
    task := zmsg.SQL("INSERT INTO feeds (id, data) VALUES (?, ?)", id, data)
    zm.CacheAndStore(ctx, id, data, task)
    
    // 4.2 异步延迟写入（高吞吐）
    zm.CacheAndDelayStore(ctx, id, data, task)
    
    // 4.3 周期聚合写入（计数器场景）
    counterTask := zmsg.Counter("feed_meta", "like_count").
        Inc(1).
        Where("id = ?", id).
        BatchKey("meta:" + id).
        Build()
    zm.CacheAndPeriodicStore(ctx, id, nil, counterTask)

    // 5. 读取（自动走多级缓存）
    result, _ := zm.Get(ctx, id)
}
```

## 核心 API

### 写入操作

| 方法 | 说明 | 一致性 | 适用场景 |
|------|------|--------|----------|
| `CacheOnly` | 仅缓存 | — | 临时数据、会话状态 |
| `CacheAndStore` | 缓存 + 立即写 DB | 强一致 | 重要数据、支付相关 |
| `CacheAndDelayStore` | 缓存 + Asynq 延迟写 | 最终一致 | 一般内容、日志 |
| `CacheAndPeriodicStore` | 缓存 + 内存聚合写 | 最终一致 | 计数器、高频更新 |

### 读取与删除

| 方法 | 说明 |
|------|------|
| `Get` | 读取数据（Bloom → L1 → L2 → DB → 回填） |
| `Del` | 删除缓存 |
| `DelStore` | 删除缓存 + 立即删除 DB |
| `DelDelayStore` | 删除缓存 + 延迟删除 DB |
| `DBHit` | 布隆过滤器快速判断 |

### ID 生成

| 方法 | 说明 |
|------|------|
| `NextID` | 生成分布式 ID（雪花算法） |

## SQL 构建器

### 原生 SQL（支持链式调用）

```go
// 基础 INSERT
task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content)

// UPSERT (PostgreSQL)
task := zmsg.SQL("INSERT INTO feeds (id, content) VALUES (?, ?)", id, content).
    OnConflict("id").
    DoUpdate("content", "updated_at")

// INSERT ... ON CONFLICT DO NOTHING
task := zmsg.SQL("INSERT INTO feeds (id) VALUES (?)", id).
    OnConflict("id").
    DoNothing()

// RETURNING
task := zmsg.SQL("INSERT INTO feeds (id) VALUES (?)", id).
    OnConflict("id").
    DoNothing().
    Returning("id", "created_at")
```

### 聚合操作（内存聚合 + 批量写入）

**Counter 计数器**：

```go
// 点赞 +1
task := zmsg.Counter("feed_meta", "like_count").
    Inc(1).
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()

// 支持的操作：Inc(n) / Dec(n) / Mul(n) / Set(n) / Clean()
```

**Slice 数组（JSONB）**：

```go
// 添加标签
task := zmsg.Slice("feed_meta", "tags").
    Add("golang").
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()

// 支持的操作：Add(val) / Del(val) / Clean()
```

**Map 对象（JSONB）**：

```go
// 设置扩展字段
task := zmsg.Map("feed_meta", "extra").
    Set("source", "mobile").
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).
    Build()

// 支持的操作：Set(k, v) / Del(k)
```

### 聚合操作一览

| 类型 | 操作 | 说明 | 聚合策略 |
|------|------|------|----------|
| Counter | `Inc(n)` / `Dec(n)` | 计数器增减 | 内存累加，合并为一次 SQL |
| Counter | `Mul(n)` / `Set(n)` | 乘法 / 设置 | 覆盖之前的聚合 |
| Counter | `Clean()` | 清零 | 覆盖之前的聚合 |
| Slice | `Add(val)` | 数组追加 | 收集所有元素，一次批量追加 |
| Slice | `Del(val)` | 数组删除 | 收集所有元素，一次批量删除 |
| Map | `Set(k, v)` | 设置键值 | 收集所有 KV，一次批量更新 |
| Map | `Del(k)` | 删除键 | 收集所有键，一次批量删除 |

## 配置说明

```yaml
# PostgreSQL
postgres:
  dsn: "postgresql://user:pass@localhost:5432/zmsg?sslmode=disable"
  max_open_conns: 100
  max_idle_conns: 10

# Redis (L2 缓存)
redis:
  addr: "localhost:6379"
  password: ""
  db: 0

# 异步队列 (Asynq)
queue:
  addr: "localhost:6379"
  concurrency: 10
  task_delay: 100ms              # 延迟写入默认延迟
  fallback_to_sync: true         # 入队失败时同步写入

# 缓存配置
cache:
  l1_max_cost: 104857600         # L1 缓存大小 (100MB)
  l1_num_counters: 10000000      # L1 计数器数量
  default_ttl: 24h               # 默认 TTL
  
  # 布隆过滤器
  bloom_capacity: 1000000        # 容量
  bloom_error_rate: 0.01         # 误判率
  bloom_sync_interval: 30s       # Redis 同步间隔
  bloom_enable_local_cache: true # 本地缓存加速

# 批处理（周期写入）
batch:
  size: 1000                     # 触发 flush 的任务数
  interval: 5s                   # 周期 flush 间隔
  max_queue_size: 10000          # 队列长度阈值
  writer_shards: 16              # 分片数量（降低锁竞争）
  flush_timeout: 30s             # flush 超时

# ID 生成器
id:
  prefix: "feed"                 # 默认前缀
  node_ttl: 60s                  # 节点心跳 TTL

# 日志
log:
  level: "info"                  # debug / info / warn / error
  encoding: "json"               # json / console
  metrics_enabled: true          # 指标收集
```

## 运行示例

```bash
# 启动依赖服务
docker-compose up -d

# 运行示例
go run cmd/main.go
```

## 最佳实践

### 写入策略选择

```
是否需要强一致？
    ├── 是 → CacheAndStore（支付、订单等）
    └── 否 → 是否高频更新？
                ├── 是 → CacheAndPeriodicStore（计数器、状态）
                └── 否 → CacheAndDelayStore（一般内容）
```

### 计数器场景优化

```go
// 高频点赞场景：内存聚合，5秒 flush 一次
// 1000 次点赞 → 1 次 SQL: UPDATE feed_meta SET like_count = like_count + 1000
task := zmsg.Counter("feed_meta", "like_count").
    Inc(1).
    Where("id = ?", feedID).
    BatchKey("like:" + feedID).  // 相同 BatchKey 会聚合
    Build()
zm.CacheAndPeriodicStore(ctx, feedID, nil, task)
```

### 布隆过滤器配置

```yaml
# 预估数据量 100万，误判率 1%
bloom_capacity: 1000000
bloom_error_rate: 0.01
# 内存占用约 1.2MB
```

## 监控指标

zmsg 内置 metrics 收集，可通过日志或接入 Prometheus 获取：

- `cache_hit_total{level="l1|l2|db"}` — 缓存命中计数
- `cache_miss_total` — 缓存未命中计数
- `write_latency_seconds` — 写入延迟
- `query_latency_seconds` — 查询延迟
- `queue_enqueue_total` — 队列入队计数
- `batch_flush_total` — 批量 flush 计数

## License

MIT
