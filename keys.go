/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-redis-adapter\keys.go
 * @Description: Redis Key 管理器 - 统一管理策略存储的 Key 命名规则
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"strings"
)

// Redis Key 默认常量
const (
	DefaultKeyPrefix = "casbin:policy:" // 默认策略 Key 前缀
)

// KeyBuilder Redis Key 构建器
// 统一管理策略存储的 Key 命名规则，支持自定义前缀
// 数据结构说明：
//   - 策略内容: {prefix}p:alice:data1:read -> "p, alice, data1, read"
//   - 策略集合: {prefix}set -> Set{所有策略key}
//   - 类型集合: {prefix}types -> Set{p, g}
type KeyBuilder struct {
	prefix  string // Key 前缀，用于多租户隔离
	setKey  string // 策略集合 Key，存储所有策略的 Key 列表
	typeKey string // 策略类型集合 Key，存储所有策略类型（p, g 等）
}

// NewKeyBuilder 创建 Key 构建器
// prefix 为 Key 前缀，为空时使用默认前缀 "casbin:policy:"
func NewKeyBuilder(prefix string) *KeyBuilder {
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return &KeyBuilder{
		prefix:  prefix,
		setKey:  prefix + "set",
		typeKey: prefix + "types",
	}
}

// PolicyKey 根据策略行生成唯一的 Redis Key
// 将策略行中的逗号替换为冒号，生成层级化的 Key
// 例如: "p, alice, data1, read" -> "casbin:policy:p:alice:data1:read"
func (kb *KeyBuilder) PolicyKey(line string) string {
	parts := strings.Split(line, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return kb.prefix + strings.Join(parts, ":")
}

// SetKey 返回策略集合 Key
// 该 Key 对应一个 Redis Set，存储所有策略的 Key 列表
func (kb *KeyBuilder) SetKey() string {
	return kb.setKey
}

// TypeKey 返回策略类型集合 Key
// 该 Key 对应一个 Redis Set，存储所有策略类型（p, g 等）
func (kb *KeyBuilder) TypeKey() string {
	return kb.typeKey
}

// ExtractPType 从策略行提取策略类型（PType）
// 例如: "p, alice, data1, read" -> "p"
func ExtractPType(line string) string {
	parts := strings.SplitN(line, ",", 2)
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}
