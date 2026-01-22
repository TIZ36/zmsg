# zmsg - 高性能消息存储解决方案库

zmsg 是一个为大规模消息/Feed系统设计的存储解决方案库，提供了多级缓存、异步持久化、批处理聚合等高级特性。

## 特性

- **多级缓存**: L1(本地内存) + L2(Redis) + 布隆过滤器
- **自动ID生成**: 基于雪花算法，自动节点管理
- **异步持久化**: Redis队列 + 批量写入
- **批处理聚合**: 高并发计数器聚合（如点赞、关注）
- **SQL集成**: 支持自定义 PostgreSQL SQL
- **一致性级别**: 最终一致性 / 强一致性
- **监控指标**: Prometheus 指标暴露

## 快速开始

### 安装

```bash
go get github.com/tiz36/zmsg
import "github.com/tiz36/zmsg"
```

### 基本用法
ctx := context.Background()

// 初始化
cfg := zmsg.DefaultConfig()
cfg.PostgresDSN = "postgresql://user:pass@localhost/db"
cfg.RedisAddr = "localhost:6379"

zm, err := zmsg.New(ctx, cfg)
if err != nil {
    panic(err)
}
defer zm.Close()

// 发布 Feed
feedID, _ := zm.NextID(ctx, "feed")
feedData := []byte(`{"content": "Hello"}`)

sqlTask := &zmsg.SQLTask{
    Query: "INSERT INTO feeds VALUES ($1, $2)",
    Params: []interface{}{feedID, feedData},
}

id, err := zm.CacheAndStore(ctx, feedID, feedData, sqlTask)

// 查询
data, err := zm.Get(ctx, feedID)

// 缓存操作
CacheOnly(ctx, key, value, opts...)
CacheAndStore(ctx, key, value, sqlTask, opts...)
CacheAndDelayStore(ctx, key, value, sqlTask, opts...)

// 删除操作
Del(ctx, key)
DelStore(ctx, key, sqlTask)
DelDelayStore(ctx, key, sqlTask)

// 更新操作
Update(ctx, key, value)
UpdateStore(ctx, key, value, sqlTask)

// 查询
Get(ctx, key)

// 工具方法
NextID(ctx, prefix)
DBHit(ctx, key)
SQLExec(ctx, sqlTask)

### 配置

postgres_dsn: "postgresql://user:pass@localhost/zmsg"
redis_addr: "localhost:6379"
queue_addr: "localhost:6380"

l1_max_cost: 1000000
default_ttl: 24h

batch_size: 1000
batch_interval: 5s

id_prefix: "feed"
metrics_enabled: true

---

这个完整的项目包含了：

1. **完整的目录结构** - 符合 Go 项目标准
2. **所有接口定义** - 支持你提到的 10 个核心能力
3. **默认实现** - 每个文件都有完整的实现
4. **完整的测试** - 包括单元测试和集成测试
5. **可编译运行** - 包含 Makefile、Docker 配置
6. **使用示例** - cmd/example/main.go 展示所有功能

要运行这个项目：

```bash
# 启动依赖服务
docker-compose up -d

# 运行测试
make test

# 运行示例
make run

# 清理
make clean
所有代码都可以直接复制到你的项目中，只需要调整包名和数据库配置即可运行。

```