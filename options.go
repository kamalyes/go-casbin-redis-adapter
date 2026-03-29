/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-redis-adapter\options.go
 * @Description: Redis 适配器配置选项 - 支持函数式选项模式
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"time"

	"github.com/kamalyes/go-logger"
)

// 默认配置常量
const (
	DefaultTTL = 0 // 默认策略过期时间，0 表示永不过期
)

// Option 适配器配置选项函数
// 使用函数式选项模式，支持链式调用
type Option func(*Adapter)

// WithKeyPrefix 设置 Redis Key 前缀
// 默认为 "casbin:policy:"，多租户场景下可设置不同前缀实现隔离
// 例如: "tenant1:casbin:" 或 "app:auth:"
func WithKeyPrefix(prefix string) Option {
	return func(a *Adapter) {
		if prefix != "" {
			a.keys = NewKeyBuilder(prefix)
		}
	}
}

// WithTTL 设置策略过期时间
// 默认为 0（永不过期），适用于需要定期刷新策略的场景
// 设置后所有写入的策略都会在指定时间后自动过期
func WithTTL(ttl time.Duration) Option {
	return func(a *Adapter) {
		a.ttl = ttl
	}
}

// WithLogger 设置日志记录器
// 默认使用空日志（不输出），生产环境建议传入 go-logger 实例
func WithLogger(l logger.ILogger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.logger = l
		}
	}
}
