# zmsg

<div align="center">

**High-Performance Feed Storage Engine for Social Scenarios**

[![Go Version](https://img.shields.io/badge/go-1.23+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tiz36/zmsg?style=flat-square)](https://goreportcard.com/report/github.com/tiz36/zmsg)

[Features](#features) • [Quick Start](#quick-start) • [Performance](#performance) • [Architecture](#architecture) • [API Reference](#api-reference)

</div>

---

## 📖 Introduction

`zmsg` is a high-throughput storage engine designed for social feed systems. It balances data consistency and write throughput using **multi-level caching**, **async queues** (Asynq), and **in-memory aggregation**.

### Key Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| **Read Latency** | **~340ns** | L1 Cache hit (In-memory) |
| **Write Latency** | **~60µs** | Async Periodic Write |
| **Throughput** | **15K+ ops/sec** | Single node write throughput |
| **Consistency** | Tunable | Strong (Sync) / Eventual (Async) |

### Core Features

- **Fluent API**: Modern, chainable builder pattern for intuitive CRUD operations.
- **Multi-Level Cache**: L1 (Ristretto) + L2 (Redis) with automatic cache refilling.
- **Write Strategies**:
    - **Sync**: Strong consistency for critical data.
    - **Periodic**: High throughput batch writing (Aggregation).
    - **Serialized**: Distributed ordered execution for hot keys.
- **Deep Merging**: Automatically merges JSON/Map updates in background batches.
- **Distributed ID**: Integrated Snowflake ID generator.

## 🚀 Quick Start

### Installation

```bash
go get github.com/tiz36/zmsg
```

### Basic Usage

```go
package main

import (
    "context"
    "github.com/tiz36/zmsg/zmsg"
)

type Feed struct {
    ID      string `db:"id,pk"`
    Content string `db:"content"`
}

func main() {
    ctx := context.Background()
    
    // 1. Initialize
    cfg, _ := zmsg.LoadConfig("config.yaml") // or zmsg.DefaultConfig()
    zm, _ := zmsg.New(ctx, cfg)
    defer zm.Close()

    // 2. Generate ID
    id, _ := zm.NextID(ctx, "feed")

    // 3. Save (Write-Through Cache + DB)
    feed := Feed{ID: id, Content: "Hello World"}
    _ = zm.Table("feeds").CacheKey(id).Save(feed)

    // 4. Read (Automatic Cache Hit)
    // Returns JSON bytes, hits L1 cache in ~300ns
    data, _ := zm.Table("feeds").CacheKey(id).Query()
}
```

## 📊 Performance

Benchmarks run on Apple M3 Pro (Go 1.23):

```text
BenchmarkThroughput/Query-11                     339.1 ns/op   (L1 Hit)
BenchmarkThroughput/Save_Sync-11                 204,026 ns/op (DB Write)
BenchmarkThroughput/Save_Periodic-11             59,780 ns/op  (Async Batch)
BenchmarkThroughput/Counter_Periodic-11          62,837 ns/op  (Aggregated Inc)
BenchmarkSerialization/WithSerialize-11          131,695 ns/op (Distributed Lock)
```

> Run `make bench` to verify on your machine.

## 🏗️ Architecture

```mermaid
graph TD
    App[Application] --> API[Fluent API]
    
    subgraph Read Path
    API --> L1[L1 Cache (Ristretto)]
    L1 -->|Miss| L2[L2 Cache (Redis)]
    L2 -->|Miss| DB[(PostgreSQL)]
    end
    
    subgraph Write Path
    API -->|Sync| DB
    API -->|Async| Queue[Asynq / Memory Batch]
    Queue -->|Aggregation| DB
    end
```

## 📚 API Reference

### 1. Fluent Builder

The `zm.Table("name")` builder is the entry point for most operations.

| Method | Description |
|--------|-------------|
| `CacheKey(key)` | Sets the cache key (L1/L2). Required for cache operations. |
| `Serialize(key)` | Ensures distributed serial execution for this key. |
| `Where(cond, args...)` | Custom WHERE clause for SQL generation. |
| `Save(struct)` | Saves specific struct/map data. |
| `Del()` | Deletes from cache and DB. |
| `Query()` | Fetches data (Cache -> DB). |

### 2. Write Strategies

You can choose different consistency levels per operation:

#### Synchronous (Default)
Writes to Cache and DB immediately. Strong consistency.
```go
zm.Table("users").CacheKey("u:1").Save(user)
```

#### Periodic Override (Last-Write-Wins)
Buffers writes in memory and flushes the latest version periodically. High throughput for frequent updates where intermediate states don't matter.
```go
zm.Table("users").CacheKey("u:1").
    PeriodicOverride().
    Save(user)
```

#### Periodic Merge (Deep JSON Merge)
Merges map/JSON updates in memory. Useful for partial updates to a JSONB column.
```go
// Updates meta->'v' without overwriting other fields
zm.Table("users").CacheKey("u:1").
    PeriodicMerge().
    UpdateColumns(map[string]any{
        "meta": map[string]any{"v": 2},
    })
```

#### Periodic Counter (High Performance)
Aggregates increments in memory.
```go
zm.Table("feed_meta").CacheKey("meta:101").
    PeriodicCount().
    UpdateColumn().Column("like_count").
    Do(zmsg.Add(), 1)
```

### 3. Distributed Serialization

Ensures that concurrent requests for the same key are processed in order across multiple server nodes. Uses distributed locking/queuing behind the scenes.

```go
// Safe concurrent balance update
zm.Table("wallets").CacheKey("w:1").
    Serialize("w:1"). // Lock Key
    UpdateColumn().Column("balance").
    Do(zmsg.Add(), 100)
```

## ⚙️ Configuration

Minimal `config.yaml` example:

```yaml
postgres:
  dsn: "postgres://user:pass@localhost:5432/zmsg?sslmode=disable"

redis:
  addr: "localhost:6379"

queue:
  concurrency: 16 # Number of concurrent workers

cache:
  l1_max_cost: 104857600 # 100MB
```

## 📄 License

MIT © [TIZ36](https://github.com/TIZ36)