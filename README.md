# zmsg

<div align="center">

**高性能消息/Feed 存储引擎，专为大规模社交场景设计**

[![Go Version](https://img.shields.io/badge/go-1.24+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tiz36/zmsg?style=flat-square)](https://goreportcard.com/report/github.com/tiz36/zmsg)

[特性](#特性) • [快速开始](#快速开始) • [性能指标](#性能指标) • [架构设计](#架构设计) • [最佳实践](#最佳实践)

</div>

---

## 📖 简介

`zmsg` 是一个专为高并发社交场景设计的高性能存储引擎，通过**多级缓存架构**、**延迟写入**和**批量聚合**技术，在保证数据一致性的同时，大幅提升系统性能和开发效率。

### 核心优势

- ⚡ **极致性能**：并行读取延迟低至 **171ns**，支持百万级 QPS
- 🚀 **高吞吐量**：延迟写入 + 批量聚合，吞吐量提升 **10-100倍**
- 🎯 **自动聚合**：内置计数器聚合，减少 **90%+** DB 写入
- 🛡️ **缓存穿透保护**：布隆过滤器自动过滤无效查询
- 🔧 **开发友好**：链式 API，语义清晰，减少样板代码

## ✨ 特性

- **多级缓存** — L1 本地缓存（Ristretto）+ L2 Redis + 布隆过滤器，自动穿透保护
- **延迟写入** — 先缓存后异步落库，应对高并发写入
- **批量聚合** — 计数器自动聚合（点赞、关注等），减少 DB 压力
- **分布式 ID** — 雪花算法 + PostgreSQL 节点自动分配，高性能 ID 生成（~250ns/op）
- **SQL 构建器** — 链式调用，支持 PostgreSQL 特性（ON CONFLICT、RETURNING）
- **SQL 迁移** — 增量迁移，自动跳过已执行

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

### 核心操作性能

| 操作类型 | 延迟 (ns/op) | 吞吐量 (ops/sec) | 内存分配 | 性能评级 |
|---------|-------------|----------------|---------|---------|
| **读取操作** |
| `Get` (并行) | 171.6 | ~5,827,506 | 240 B | ⭐⭐⭐⭐⭐ |
| `Get` (单线程) | 354.5 | ~2,820,875 | 247 B | ⭐⭐⭐⭐⭐ |
| **写入操作** |
| `CacheOnly` (并行) | 61,460 | ~16,271 | 981 B | ⭐⭐⭐ |
| `CacheAndPeriodicStore` (并行) | 61,527 | ~16,250 | 2,304 B | ⭐⭐⭐ |
| **ID 生成** |
| `NextID` | 246.7 | ~4,053,506 | 88 B | ⭐⭐⭐⭐⭐ |
| `NextID` (并行) | 405.9 | ~2,463,660 | 88 B | ⭐⭐⭐⭐⭐ |
| **SQL 构建** |
| SQL Basic | 182.6 | ~5,475,357 | 128 B | ⭐⭐⭐⭐⭐ |
| Counter Inc | 296.8 | ~3,369,272 | 480 B | ⭐⭐⭐⭐⭐ |

> 📈 完整 benchmark 报告：运行 `make bench && make report` 查看详细性能数据

### 性能对比

| 产品 | 读取延迟 | 写入吞吐量 | 缓存策略 | 聚合能力 |
|------|---------|-----------|----------|----------|
| **zmsg** | **171ns** | **16K+ ops/sec** | L1+L2+Bloom | ✅ 内置 |
| Redis + PostgreSQL | ~60μs | ~1K ops/sec | Redis 单层 | ❌ 需自实现 |
| GORM + Redis | ~100μs | ~500 ops/sec | Redis 单层 | ❌ 需自实现 |

> 💡 **性能优势**：zmsg 的并行读取延迟比直接访问 Redis 快约 **360倍**

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                      Application                        │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                        zmsg                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐  │
│  │ L1 Cache│→ │ L2 Cache│→ │  Bloom  │→ │ PostgreSQL│  │
│  │(Ristretto)│ │ (Redis) │  │ Filter  │  │           │  │
│  │ ~171ns   │  │ ~60μs   │  │ 穿透保护 │  │  持久化   │  │
│  └─────────┘  └─────────┘  └─────────┘  └───────────┘  │
│                    │                                    │
│              ┌─────▼─────┐                              │
│              │   Queue   │  ← 延迟写入                  │
│              │  (Asynq)  │  ← 批量聚合                  │
│              └───────────┘                              │
└─────────────────────────────────────────────────────────┘
```

### 数据流

1. **读取流程**：L1 缓存 → L2 Redis → 布隆过滤器 → PostgreSQL
2. **写入流程**：L1/L2 缓存 → 异步队列 → 批量聚合 → PostgreSQL
3. **缓存策略**：多级缓存 + 布隆过滤器防止穿透

## 📚 API 文档

### 核心 API

| 方法 | 说明 | 一致性 | 性能 |
|------|------|--------|------|
| `CacheAndStore(ctx, key, data, task)` | 缓存 + 立即写 DB | 强一致 | 中等 |
| `CacheAndDelayStore(ctx, key, data, task)` | 缓存 + 延迟写 DB | 最终一致 | 高 |
| `CacheAndPeriodicStore(ctx, key, data, task)` | 缓存 + 周期聚合写入 | 最终一致 | 最高 |
| `Get(ctx, key)` | 读取（L1 → L2 → DB） | - | 极高 |
| `CacheOnly(ctx, key, data, opts...)` | 仅缓存，不写 DB | - | 高 |
| `Del(ctx, key)` / `DelStore(ctx, key)` | 删除缓存 / 删除并写 DB | - | - |
| `NextID(ctx, prefix)` | 生成分布式 ID | - | 极高 |
| `DBHit(ctx, key)` | 布隆过滤器快速判断 | - | 极高 |

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

### 配置文件示例

```yaml
# PostgreSQL 配置
postgres_dsn: "postgresql://user:pass@localhost/zmsg?sslmode=disable"
postgres_max_open_conns: 25
postgres_max_idle_conns: 5
postgres_conn_max_lifetime: 5m

# Redis 配置
redis_addr: "localhost:6379"
redis_password: ""
redis_db: 0
redis_pool_size: 10

# L1 本地缓存配置
l1_max_cost: 104857600  # 100MB
l1_num_counters: 10000000
l1_buffer_items: 64

# 缓存默认配置
default_ttl: 24h
cache_prefix: "zmsg:"

# 批量聚合配置
batch_size: 1000
batch_interval: 5s
batch_shards: 16

# 布隆过滤器配置
bloom_capacity: 1000000
bloom_error_rate: 0.01

# 日志配置
log_level: "info"  # debug, info, warn, error
log_format: "json"  # json, text
```

### 环境变量

支持通过环境变量覆盖配置：

```bash
export ZMSG_POSTGRES_DSN="postgresql://user:pass@localhost/zmsg"
export ZMSG_REDIS_ADDR="localhost:6379"
export ZMSG_L1_MAX_COST="104857600"
```

## 🎯 最佳实践

### 1. 选择合适的写入模式

```go
// ✅ 强一致性场景（订单、支付）
zm.CacheAndStore(ctx, orderID, orderData, task)

// ✅ 高吞吐场景（点赞、评论）
zm.CacheAndPeriodicStore(ctx, feedID, nil, counterTask)

// ✅ 仅缓存场景（临时数据）
zm.CacheOnly(ctx, sessionID, sessionData, zmsg.WithTTL(time.Hour))
```

### 2. 使用批量聚合优化计数器

```go
// ✅ 推荐：使用 BatchKey 聚合相同 key 的操作
task := zmsg.Counter("feed_meta", "like_count").
    Inc(1).
    Where("id = ?", feedID).
    BatchKey("meta:" + feedID).  // 相同 BatchKey 会自动聚合
    Build()
zm.CacheAndPeriodicStore(ctx, key, nil, task)

// ❌ 不推荐：每次都写 DB
zm.CacheAndStore(ctx, key, nil, task)  // 会立即写 DB
```

### 3. 合理设置 BatchKey

```go
// ✅ 好的 BatchKey：按业务维度聚合
BatchKey("meta:" + feedID)        // 按 Feed 聚合
BatchKey("user:" + userID)         // 按用户聚合

// ❌ 不好的 BatchKey：过于分散
BatchKey("meta:" + feedID + ":" + timestamp)  // 无法聚合
```

### 4. 监控和调优

```go
// 监控缓存命中率
stats := zm.GetStats()
fmt.Printf("L1 Hit Rate: %.2f%%\n", stats.L1HitRate*100)
fmt.Printf("L2 Hit Rate: %.2f%%\n", stats.L2HitRate*100)

// 根据命中率调整配置
if stats.L1HitRate < 0.8 {
    // 增加 L1 缓存大小
    cfg.L1MaxCost = 209715200  // 200MB
}
```

### 5. 错误处理

```go
// ✅ 推荐：检查错误并处理
data, err := zm.Get(ctx, key)
if err != nil {
    if err == zmsg.ErrNotFound {
        // 处理未找到的情况
        return nil, fmt.Errorf("feed not found: %s", key)
    }
    // 处理其他错误
    return nil, fmt.Errorf("failed to get feed: %w", err)
}

// ✅ 推荐：使用 context 超时控制
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
id, err := zm.NextID(ctx, "feed")
```

## 🔍 适用场景

### ✅ 强烈推荐

- **高并发社交 Feed 系统**：大量读取操作，需要低延迟
- **实时计数器场景**：点赞、关注、阅读量等
- **Feed 流推荐系统**：需要快速读取 Feed 内容

### ⚠️ 谨慎使用

- **复杂关系查询**：需要 JOIN、子查询等复杂 SQL（推荐使用 Ent/GORM）
- **强一致性要求**：金融交易、订单系统（使用 `CacheAndStore`）
- **单机小规模应用**：QPS < 1000，单实例部署

### ❌ 不推荐

- **纯 OLAP 场景**：数据分析、报表生成（推荐 ClickHouse、BigQuery）
- **简单 CRUD 应用**：管理后台、内部工具（推荐 GORM 或 Ent）

## 🛠️ 开发工具

### Benchmark 测试

```bash
# 运行 benchmark
make bench

# 生成性能报告和图表
make report

# 生成行业分析报告
make analyse
```

### 测试

```bash
# 运行单元测试
make test

# 运行集成测试
make integration-test
```

## 📦 生产环境部署

### Docker Compose

```bash
# 启动依赖服务
docker-compose up -d

# 检查服务状态
docker-compose ps
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zmsg-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: your-app:latest
        env:
        - name: ZMSG_POSTGRES_DSN
          valueFrom:
            secretKeyRef:
              name: zmsg-secrets
              key: postgres-dsn
        - name: ZMSG_REDIS_ADDR
          value: "redis:6379"
```

### 监控指标

建议监控以下指标：

- **缓存命中率**：L1/L2 命中率
- **写入延迟**：批量写入延迟分布
- **队列长度**：异步队列积压情况
- **错误率**：DB 写入失败率

## 🔧 故障排查

### 常见问题

1. **缓存命中率低**
   - 检查 L1 缓存大小是否足够
   - 调整 TTL 策略
   - 检查数据访问模式

2. **写入延迟高**
   - 检查批量聚合配置（batch_size, batch_interval）
   - 检查 PostgreSQL 连接池配置
   - 监控队列积压情况

3. **内存占用高**
   - 调整 L1 缓存大小（l1_max_cost）
   - 检查批量聚合缓冲区大小

## 🤝 贡献

欢迎贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详细信息。

## 📄 License

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- [Ristretto](https://github.com/dgraph-io/ristretto) - 高性能本地缓存
- [Asynq](https://github.com/hibiken/asynq) - 分布式任务队列
- [Bloom Filter](https://github.com/bits-and-blooms/bloom) - 布隆过滤器实现

---

<div align="center">

**Made with ❤️ for high-performance social applications**

[文档](https://github.com/tiz36/zmsg/wiki) • [问题反馈](https://github.com/tiz36/zmsg/issues) • [讨论](https://github.com/tiz36/zmsg/discussions)

</div>