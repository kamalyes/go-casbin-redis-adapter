# go-casbin-redis-adapter

[go-casbin](https://github.com/kamalyes/go-casbin) 的 Redis 适配器，基于 [go-cachex](https://github.com/kamalyes/go-cachex) 实现策略缓存存储，同时提供 Redis Pub/Sub 分布式策略同步能力

## 安装

```bash
go get github.com/kamalyes/go-casbin-redis-adapter
```

## 基本使用（策略存储）

```go
package main

import (
    "time"

    "github.com/kamalyes/go-casbin/enforcer"
    redisadapter "github.com/kamalyes/go-casbin-redis-adapter"
    "github.com/kamalyes/go-logger"
    "github.com/redis/go-redis/v9"
)

func main() {
    log := logger.NewLogger().WithLevel(logger.INFO)

    // 1. 创建 Redis 客户端
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // 2. 创建适配器
    adapter, _ := redisadapter.NewAdapter(client,
        redisadapter.WithKeyPrefix("myapp:casbin:"),
        redisadapter.WithTTL(24*time.Hour),
        redisadapter.WithLogger(log),
    )
    defer adapter.Close()

    // 3. 创建 enforcer
    e, _ := enforcer.NewEnforcer(
        enforcer.WithModelPath("resources/rbac_model.conf"),
        enforcer.WithAdapter(adapter),
        enforcer.WithLogger(log),
    )

    // 4. 权限检查
    ok, _ := e.Enforce("alice", "data1", "read")
}
```

## 多租户使用

每个租户使用独立的 Key 前缀，实现策略隔离：

```go
// 租户1：独立前缀 + RBAC 模型
adapter1, _ := redisadapter.NewAdapter(client,
    redisadapter.WithKeyPrefix("tenant1:casbin:"),
)
e1, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/rbac_with_domains_model.conf"),
    enforcer.WithAdapter(adapter1),
    enforcer.WithLogger(log),
)

// 租户2：独立前缀 + ABAC 规则策略模型
adapter2, _ := redisadapter.NewAdapter(client,
    redisadapter.WithKeyPrefix("tenant2:casbin:"),
)
e2, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithAdapter(adapter2),
    enforcer.WithLogger(log),
)
```

## 分布式策略同步（Redis Pub/Sub）

A 节点修改策略后，通过 Redis Pub/Sub 广播变更事件，B/C/D 节点自动重载：

```go
import "github.com/kamalyes/go-casbin/policy"

// 创建 Redis 通知器
notifier, _ := redisadapter.NewRedisNotifier(client,
    policy.WithChannel("casbin:tenant1:policy:changes"),  // 每个租户独立频道
    policy.WithSource("node-1"),
)

// 创建执行器并集成通知器
e, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/rbac_model.conf"),
    enforcer.WithAdapter(adapter),
    enforcer.WithNotifier(notifier),
    enforcer.WithLogger(log),
)

// 修改策略后自动通知其他节点
_ = e.AddPolicy("alice", "data3", "read")
```

## ABAC 规则策略 + Redis 缓存

ABAC 规则策略的条件表达式缓存到 Redis，支持 TTL 自动过期：

```go
adapter, _ := redisadapter.NewAdapter(client,
    redisadapter.WithKeyPrefix("abac:casbin:"),
    redisadapter.WithTTL(1*time.Hour),  // 策略1小时后自动过期
)

e, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithAdapter(adapter),
    enforcer.WithLogger(log),
)

// 添加 ABAC 规则策略（自动缓存到 Redis）
_ = e.AddPolicy(`r.sub == "alice"`, "data1", "read")
```

## 配置选项

### Adapter 选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `WithKeyPrefix(prefix)` | 设置 Key 前缀，多租户场景下可设置不同前缀实现隔离 | `casbin:policy:` |
| `WithTTL(ttl)` | 设置过期时间 | 0 (永不过期) |
| `WithLogger(logger)` | 设置日志记录器 | 空日志 |

### Notifier 选项（通过 policy.NotifierOption 配置）

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `policy.WithChannel(name)` | Pub/Sub 频道名称 | `casbin:policy:changes` |
| `policy.WithSource(id)` | 本节点标识 | 自动生成 |
| `policy.WithBufferSize(n)` | 事件缓冲区大小 | 256 |
| `policy.WithRetry(interval, count)` | 发布重试参数 | 1s, 3次 |

## Redis 数据结构

```
casbin:policy:p:alice:data1:read  -> "p, alice, data1, read"
casbin:policy:g:alice:admin       -> "g, alice, admin"
casbin:policy_set                 -> Set{所有策略key}
casbin:policy_types               -> Set{p, g}
```

## 支持的接口

- `Adapter` - 基础加载/保存
- `FilteredAdapter` - 过滤加载
- `BatchAdapter` - 批量操作
- `UpdatableAdapter` - 更新操作

## 特性

- ✅ Pipeline 批量操作，减少网络往返
- ✅ 支持 TTL 过期
- ✅ 支持自定义 Key 前缀（多租户隔离）
- ✅ 支持过滤加载
- ✅ 内置 Redis Pub/Sub 分布式策略同步

## License

Apache-2.0
