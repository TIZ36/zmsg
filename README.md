# zmsg

高性能消息/Feed 存储引擎，专为大规模社交场景设计。

## 特性

- **多级缓存** — L1 本地 + L2 Redis + 布隆过滤器，自动穿透保护
- **延迟写入** — 先缓存后异步落库，应对高并发写入
- **批量聚合** — 计数器自动聚合（点赞、关注等），减少 DB 压力
- **分布式 ID** — 雪花算法 + PostgreSQL 节点自动分配
- **SQL 迁移** — 增量迁移，自动跳过已执行

## 安装

```bash
go get github.com/tiz36/zmsg
```

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

## SQL 构建

支持两种方式，按场景选择：

```go
// 方式一：原生 SQL（简单场景）
task := zmsg.SQL("UPDATE feeds SET content = ? WHERE id = ?", content, id)

// 方式二：Builder（复杂场景）
task := zmsg.NewBuilder().
    Insert("feeds", map[string]interface{}{"id": id, "content": content}).
    OnConflict("id").
    DoUpdate("content").
    Build()
```

## 核心 API

| 方法 | 说明 |
|------|------|
| `CacheAndStore` | 缓存 + 立即写 DB（强一致） |
| `CacheAndDelayStore` | 缓存 + 延迟写 DB（高吞吐） |
| `Get` | 读取（L1 → L2 → DB） |
| `Del` / `DelStore` | 删除缓存 / 删除并写 DB |
| `NextID` | 生成分布式 ID |
| `DBHit` | 布隆过滤器快速判断 |

## 配置示例

```yaml
postgres_dsn: "postgresql://user:pass@localhost/zmsg"
redis_addr: "localhost:6379"

l1_max_cost: 104857600  # 100MB
default_ttl: 24h

batch_size: 1000
batch_interval: 5s
```

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                      Application                        │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                        zmsg                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐  │
│  │ L1 Cache│→ │ L2 Cache│→ │  Bloom  │→ │ PostgreSQL│  │
│  │ (Local) │  │ (Redis) │  │ Filter  │  │           │  │
│  └─────────┘  └─────────┘  └─────────┘  └───────────┘  │
│                    │                                    │
│              ┌─────▼─────┐                              │
│              │   Queue   │  ← 延迟写入                  │
│              │  (Redis)  │  ← 批量聚合                  │
│              └───────────┘                              │
└─────────────────────────────────────────────────────────┘
```

## 运行示例

```bash
# 启动依赖
docker-compose up -d

# 运行
go run cmd/main.go
```

## License

MIT
