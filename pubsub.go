/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-redis-adapter\pubsub.go
 * @Description: Redis Pub/Sub 策略变更通知器 - 基于 Redis 的分布式策略同步实现
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/idgen"
	"github.com/kamalyes/go-toolbox/pkg/retry"
	"github.com/redis/go-redis/v9"
)

// 编译时接口断言，确保 RedisNotifier 实现了 PolicyNotifier 接口
var _ policy.PolicyNotifier = (*RedisNotifier)(nil)

// RedisNotifier 基于 Redis Pub/Sub 的策略变更通知器
// 解决分布式部署下的策略一致性问题：
//   - A 节点修改策略 → 通过 Redis Pub/Sub 广播变更事件
//   - B/C/D 节点收到事件 → 自动重载策略
//   - 新节点启动 → 通过 RequestSync 获取全量策略
//
// 支持特性：
//   - 自动重连：Redis 连接断开后自动重试订阅
//   - 消息去重：基于事件 ID 和来源节点过滤重复/自身事件
//   - 发布重试：发布失败时自动重试
//   - 优雅关闭：支持 context 取消和 Close 方法
type RedisNotifier struct {
	client *redis.Client          // Redis 客户端
	config *policy.NotifierConfig // 通知器配置
	logger logger.ILogger         // 日志记录器
	idgen  idgen.IDGenerator      // ID 生成器（用于事件唯一 ID）
	retry  *retry.Retry           // 发布重试器

	mu      sync.RWMutex              // 保护以下字段
	running bool                      // 是否正在运行
	cancel  context.CancelFunc        // 取消订阅的函数
	handler policy.ChangeEventHandler // 事件处理函数
	sub     *redis.PubSub             // Redis 订阅对象
}

// NewRedisNotifier 创建 Redis Pub/Sub 通知器
// client: 已初始化的 Redis 客户端
// opts: 可选配置项（频道名、节点标识、重试等）
func NewRedisNotifier(client *redis.Client, opts ...policy.NotifierOption) (*RedisNotifier, error) {
	if client == nil {
		return nil, errors.NewPolicyAdapterFailedError("redis client is nil")
	}

	// 检测 Redis 连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.NewPolicyAdapterFailedError("redis ping failed: " + err.Error())
	}

	config := policy.DefaultNotifierConfig()
	for _, opt := range opts {
		opt(config)
	}

	// 如果未设置节点标识，自动生成
	if config.Source == "unknown" {
		config.Source = fmt.Sprintf("node-%s", idgen.NewIDGenerator(idgen.GeneratorTypeUUID).GenerateRequestID())
	}

	return &RedisNotifier{
		client: client,
		config: config,
		logger: logger.NewEmptyLogger(),
		idgen:  idgen.NewIDGenerator(idgen.GeneratorTypeUUID),
		retry: retry.NewRetry().
			SetAttemptCount(config.RetryCount).
			SetInterval(config.RetryInterval),
	}, nil
}

// WithNotifierLogger 设置通知器日志记录器
func WithNotifierLogger(l logger.ILogger) func(*RedisNotifier) {
	return func(rn *RedisNotifier) {
		if l != nil {
			rn.logger = l
		}
	}
}

// Publish 发布策略变更事件到 Redis 频道
// 事件序列化为 JSON 格式广播，所有订阅该频道的节点都会收到
// 发布失败时自动重试（基于 go-toolbox/retry）
func (rn *RedisNotifier) Publish(ctx context.Context, event *ChangeEvent) error {
	// 填充事件元数据
	event.ID = rn.idgen.GenerateRequestID()
	event.Source = rn.config.Source
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// 序列化事件
	data, err := json.Marshal(event)
	if err != nil {
		return errors.NewPolicyWatchFailedError("failed to marshal event: " + err.Error())
	}

	// 带重试的发布
	var publishErr error
	retryErr := rn.retry.Do(func() error {
		publishErr = rn.client.Publish(ctx, rn.config.Channel, data).Err()
		return publishErr
	})

	if retryErr != nil {
		return errors.NewPolicyWatchFailedError("failed to publish event after retries: " + retryErr.Error())
	}

	rn.logger.DebugKV("Policy change event published",
		"channel", rn.config.Channel,
		"event_type", string(event.Type),
		"source", event.Source,
	)

	return nil
}

// Subscribe 订阅策略变更事件
// 启动后台 goroutine 持续监听 Redis 频道
// 收到事件后反序列化并调用 handler 处理
// 自动过滤自身发布的事件（基于 Source 字段）
func (rn *RedisNotifier) Subscribe(ctx context.Context, handler policy.ChangeEventHandler) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.running {
		return nil
	}

	rn.handler = handler
	subCtx, cancel := context.WithCancel(ctx)
	rn.cancel = cancel
	rn.running = true

	// 订阅 Redis 频道
	rn.sub = rn.client.Subscribe(subCtx, rn.config.Channel)

	// 启动消息接收循环
	go rn.receiveLoop(subCtx)

	rn.logger.InfoKV("Redis notifier subscribed",
		"channel", rn.config.Channel,
		"source", rn.config.Source,
	)

	return nil
}

// Unsubscribe 取消订阅策略变更事件
func (rn *RedisNotifier) Unsubscribe() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.running {
		return nil
	}

	rn.running = false
	if rn.cancel != nil {
		rn.cancel()
	}
	if rn.sub != nil {
		_ = rn.sub.Close()
	}

	rn.logger.InfoKV("Redis notifier unsubscribed", "channel", rn.config.Channel)
	return nil
}

// Close 关闭通知器，释放所有资源
func (rn *RedisNotifier) Close() error {
	return rn.Unsubscribe()
}

// receiveLoop 消息接收循环
// 持续从 Redis 订阅中读取消息，反序列化后调用 handler
// 支持自动重连：当订阅断开时自动重新订阅
func (rn *RedisNotifier) receiveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := rn.sub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			rn.logger.WarnKV("Failed to receive message, reconnecting...",
				"channel", rn.config.Channel,
				"error", err.Error(),
			)

			// 等待后重试
			time.Sleep(rn.config.RetryInterval)

			// 重新订阅
			rn.mu.Lock()
			rn.sub = rn.client.Subscribe(ctx, rn.config.Channel)
			rn.mu.Unlock()

			continue
		}

		// 反序列化事件
		var event ChangeEvent
		if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
			rn.logger.WarnKV("Failed to unmarshal event", "error", err.Error())
			continue
		}

		// 过滤自身发布的事件（避免自消费）
		if event.Source == rn.config.Source {
			rn.logger.DebugKV("Skipping self-published event",
				"event_type", string(event.Type),
				"source", event.Source,
			)
			continue
		}

		// 调用事件处理器
		rn.mu.RLock()
		handler := rn.handler
		rn.mu.RUnlock()

		if handler != nil {
			handler(&event)
		}
	}
}

// ==================== 事件类型转换 ====================

// ChangeEvent Redis 适配器专用的事件结构
// 与 policy.ChangeEvent 字段一致，用于 JSON 序列化/反序列化
type ChangeEvent = policy.ChangeEvent

// ==================== 便捷方法 ====================

// PublishPolicyAdded 发布策略添加事件
func (rn *RedisNotifier) PublishPolicyAdded(ctx context.Context, ptype string, p []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyAdded, ptype, rn.config.Source)
	event.NewPolicy = p
	return rn.Publish(ctx, event)
}

// PublishPolicyRemoved 发布策略删除事件
func (rn *RedisNotifier) PublishPolicyRemoved(ctx context.Context, ptype string, oldPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyRemoved, ptype, rn.config.Source)
	event.OldPolicy = oldPolicy
	return rn.Publish(ctx, event)
}

// PublishPolicyUpdated 发布策略更新事件
func (rn *RedisNotifier) PublishPolicyUpdated(ctx context.Context, ptype string, oldPolicy, newPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyUpdated, ptype, rn.config.Source)
	event.OldPolicy = oldPolicy
	event.NewPolicy = newPolicy
	return rn.Publish(ctx, event)
}

// PublishPolicyReload 发布策略全量重载事件
func (rn *RedisNotifier) PublishPolicyReload(ctx context.Context) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyReload, "", rn.config.Source)
	return rn.Publish(ctx, event)
}
