/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-redis-adapter\adapter.go
 * @Description: Casbin Redis 适配器 - 基于 Redis 的策略分布式缓存存储
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"context"
	"strings"
	"time"

	cachex "github.com/kamalyes/go-cachex"
	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/contextx"
	"github.com/redis/go-redis/v9"
)

// 编译时接口断言，确保 Adapter 实现了所有必需的接口
var (
	_ policy.Adapter                      = (*Adapter)(nil) // 基础适配器接口
	_ policy.FilteredAdapter              = (*Adapter)(nil) // 过滤适配器接口
	_ policy.BatchAdapter                 = (*Adapter)(nil) // 批量操作适配器接口
	_ policy.UpdatableAdapter             = (*Adapter)(nil) // 可更新适配器接口
	_ policy.PTypeUpdatableAdapter        = (*Adapter)(nil) // 按 ptype 精确更新接口
	_ policy.PTypeUpdatableContextAdapter = (*Adapter)(nil) // 带上下文的 ptype 精确更新接口
)

// replaceFilteredPoliciesScript 在 Redis 服务端原子完成过滤替换。
// 相比先 LoadPolicy 到 Go 进程再 Pipeline 写回，这里把“查旧、删旧、写新、重建类型索引”压缩成一次 EVAL。
// fieldIndex 的语义与 Casbin adapter 保持一致：0 表示 V0，而不是 p_type。
var replaceFilteredPoliciesScript = redis.NewScript(`
local set_key = KEYS[1]
local type_key = KEYS[2]

local key_prefix = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local ptype_filter = ARGV[3]
local field_index = tonumber(ARGV[4])
local filter_count = tonumber(ARGV[5])
local cursor = 6

local function trim(s)
	return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function split_policy(line)
	local parts = {}
	local start_pos = 1
	while true do
		local comma = string.find(line, ",", start_pos, true)
		if comma == nil then
			table.insert(parts, trim(string.sub(line, start_pos)))
			break
		end
		table.insert(parts, trim(string.sub(line, start_pos, comma - 1)))
		start_pos = comma + 1
	end
	return parts
end

local function policy_key(line)
	local parts = split_policy(line)
	return key_prefix .. table.concat(parts, ":")
end

local filters = {}
for i = 1, filter_count do
	filters[i] = ARGV[cursor]
	cursor = cursor + 1
end

local add_count = tonumber(ARGV[cursor])
cursor = cursor + 1

local function matches(line)
	local parts = split_policy(line)
	if ptype_filter ~= "" and parts[1] ~= ptype_filter then
		return false
	end
	for i = 1, filter_count do
		local idx = field_index + i + 1
		if parts[idx] ~= filters[i] then
			return false
		end
	end
	return true
end

local removed = 0
local keys = redis.call("SMEMBERS", set_key)
for _, key in ipairs(keys) do
	local line = redis.call("GET", key)
	if line == false then
		redis.call("SREM", set_key, key)
	elseif matches(line) then
		redis.call("DEL", key)
		redis.call("SREM", set_key, key)
		removed = removed + 1
	end
end

local added = 0
for i = 1, add_count do
	local line = ARGV[cursor]
	cursor = cursor + 1
	local parts = split_policy(line)
	if ptype_filter == "" or parts[1] == ptype_filter then
		local key = policy_key(line)
		if ttl_ms > 0 then
			redis.call("SET", key, line, "PX", ttl_ms)
		else
			redis.call("SET", key, line)
		end
		redis.call("SADD", set_key, key)
		added = added + 1
	end
end

redis.call("DEL", type_key)
keys = redis.call("SMEMBERS", set_key)
for _, key in ipairs(keys) do
	local line = redis.call("GET", key)
	if line == false then
		redis.call("SREM", set_key, key)
	else
		local parts = split_policy(line)
		if parts[1] ~= nil and parts[1] ~= "" then
			redis.call("SADD", type_key, parts[1])
		end
	end
end

return {removed, added}
`)

// Adapter 基于 Redis 的策略存储适配器
// 使用 Redis 的 String + Set 数据结构存储策略
// 支持分布式缓存、TTL 过期、自定义 Key 前缀（多租户隔离）
// 普通写操作使用 Pipeline 批量执行，过滤替换操作使用 Lua 原子执行
type Adapter struct {
	client   *redis.Client  // Redis 客户端
	handler  cachex.Handler // go-cachex 处理器（用于缓存管理）
	logger   logger.ILogger // 日志记录器
	keys     *KeyBuilder    // Key 构建器，管理 Redis Key 的生成
	ttl      time.Duration  // 策略过期时间，0 表示永不过期
	filtered bool           // 是否已使用过滤加载
}

// NewAdapter 创建 Redis 适配器
// client: 已初始化的 Redis 客户端
// opts: 可选配置项（Key 前缀、TTL、日志等）
// 创建时会自动 Ping 检测连接是否可用
func NewAdapter(client *redis.Client, opts ...Option) (*Adapter, error) {
	if client == nil {
		return nil, errors.NewPolicyAdapterFailedError("redis client is nil")
	}

	// 检测 Redis 连接是否可用
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.NewPolicyAdapterFailedError("redis ping failed: " + err.Error())
	}

	// 初始化 go-cachex 的 Redis 处理器
	handler, err := cachex.NewRedisHandler(client.Options())
	if err != nil {
		return nil, errors.NewPolicyAdapterFailedError("redis handler: " + err.Error())
	}

	a := &Adapter{
		client:  client,
		handler: handler,
		logger:  logger.NewEmptyLogger(),
		keys:    NewKeyBuilder(DefaultKeyPrefix),
		ttl:     DefaultTTL,
	}

	// 应用可选配置
	for _, opt := range opts {
		opt(a)
	}

	a.logger.InfoKV("Redis adapter initialized", "addr", client.Options().Addr)
	return a, nil
}

// NewAdapterWithConfig 通过 Redis 配置创建适配器
// 适用于没有现成 Redis 客户端的场景，内部自动创建客户端
func NewAdapterWithConfig(opts *redis.Options, adapterOpts ...Option) (*Adapter, error) {
	if opts == nil || opts.Addr == "" {
		return nil, errors.NewPolicyAdapterFailedError("redis options is nil or addr is empty")
	}

	client := redis.NewClient(opts)
	return NewAdapter(client, adapterOpts...)
}

// ==================== WithCtx 方法（核心实现） ====================
// 所有 WithCtx 方法接受 context.Context 参数，支持超时控制和链路追踪
// 使用 contextx.OrBackground 确保 ctx 不为 nil

// LoadPolicyWithCtx 从 Redis 加载所有策略规则
// 通过 SMEMBERS 获取策略集合中的所有 Key，再逐个 GET 获取策略内容
func (a *Adapter) LoadPolicyWithCtx(ctx context.Context) ([]string, error) {
	ctx = contextx.OrBackground(ctx)

	// 获取策略集合中的所有 Key
	keys, err := a.client.SMembers(ctx, a.keys.SetKey()).Result()
	if err != nil {
		return nil, errors.NewPolicyLoadFailedError("smembers: " + err.Error())
	}

	// 逐个获取策略内容
	policies := make([]string, 0, len(keys))
	for _, key := range keys {
		val, err := a.client.Get(ctx, key).Result()
		if err == redis.Nil {
			// Key 已过期或不存在，跳过
			continue
		}
		if err != nil {
			return nil, errors.NewPolicyLoadFailedError("get: " + err.Error())
		}
		policies = append(policies, val)
	}

	a.logger.InfoKV("Policies loaded from Redis", "count", len(policies))
	return policies, nil
}

// SavePolicyWithCtx 将所有策略保存到 Redis（先清空再写入）
// 使用 Pipeline 批量执行，减少网络往返
func (a *Adapter) SavePolicyWithCtx(ctx context.Context, policies []string) error {
	ctx = contextx.OrBackground(ctx)

	// 先清空所有现有策略
	if err := a.clearAll(ctx); err != nil {
		return errors.NewPolicyClearFailedError(err.Error())
	}

	if len(policies) == 0 {
		return nil
	}

	// 使用 Pipeline 批量写入
	pipe := a.client.Pipeline()
	typeSet := make(map[string]struct{})

	for _, p := range policies {
		key := a.keys.PolicyKey(p)
		pipe.Set(ctx, key, p, a.ttl)
		pipe.SAdd(ctx, a.keys.SetKey(), key)
		if ptype := policy.ExtractPType(p); ptype != "" {
			typeSet[ptype] = struct{}{}
		}
	}

	// 记录策略类型集合
	for ptype := range typeSet {
		pipe.SAdd(ctx, a.keys.TypeKey(), ptype)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicySaveFailedError(err.Error())
	}

	a.logger.InfoKV("Policies saved to Redis", "count", len(policies))
	return nil
}

// AddPolicyWithCtx 向 Redis 添加单条策略
// 同时更新策略集合和类型集合
func (a *Adapter) AddPolicyWithCtx(ctx context.Context, line string) error {
	ctx = contextx.OrBackground(ctx)
	key := a.keys.PolicyKey(line)

	pipe := a.client.Pipeline()
	pipe.Set(ctx, key, line, a.ttl)
	pipe.SAdd(ctx, a.keys.SetKey(), key)
	if ptype := policy.ExtractPType(line); ptype != "" {
		pipe.SAdd(ctx, a.keys.TypeKey(), ptype)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyAddFailedError(err.Error())
	}

	a.logger.DebugKV("Policy added to Redis", "policy", line)
	return nil
}

// RemovePolicyWithCtx 从 Redis 删除单条策略
// 同时从策略集合中移除对应的 Key
func (a *Adapter) RemovePolicyWithCtx(ctx context.Context, line string) error {
	ctx = contextx.OrBackground(ctx)
	key := a.keys.PolicyKey(line)

	pipe := a.client.Pipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, a.keys.SetKey(), key)

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyRemoveFailedError(err.Error())
	}

	a.logger.DebugKV("Policy removed from Redis", "policy", line)
	return nil
}

// AddPoliciesWithCtx 批量添加策略到 Redis
// 使用 Pipeline 一次性提交所有操作
func (a *Adapter) AddPoliciesWithCtx(ctx context.Context, lines []string) error {
	if len(lines) == 0 {
		return nil
	}

	ctx = contextx.OrBackground(ctx)
	pipe := a.client.Pipeline()

	for _, line := range lines {
		key := a.keys.PolicyKey(line)
		pipe.Set(ctx, key, line, a.ttl)
		pipe.SAdd(ctx, a.keys.SetKey(), key)
		if ptype := policy.ExtractPType(line); ptype != "" {
			pipe.SAdd(ctx, a.keys.TypeKey(), ptype)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyBatchAddFailedError(err.Error())
	}

	a.logger.DebugKV("Policies batch added to Redis", "count", len(lines))
	return nil
}

// RemovePoliciesWithCtx 批量从 Redis 删除策略
func (a *Adapter) RemovePoliciesWithCtx(ctx context.Context, lines []string) error {
	if len(lines) == 0 {
		return nil
	}

	ctx = contextx.OrBackground(ctx)
	pipe := a.client.Pipeline()

	for _, line := range lines {
		key := a.keys.PolicyKey(line)
		pipe.Del(ctx, key)
		pipe.SRem(ctx, a.keys.SetKey(), key)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyBatchRemoveFailedError(err.Error())
	}

	a.logger.DebugKV("Policies batch removed from Redis", "count", len(lines))
	return nil
}

// UpdatePolicyWithCtx 更新单条策略
// 先删除旧策略，再添加新策略
func (a *Adapter) UpdatePolicyWithCtx(ctx context.Context, oldLine, newLine string) error {
	ctx = contextx.OrBackground(ctx)

	oldKey := a.keys.PolicyKey(oldLine)
	newKey := a.keys.PolicyKey(newLine)

	pipe := a.client.Pipeline()
	// 删除旧策略
	pipe.Del(ctx, oldKey)
	pipe.SRem(ctx, a.keys.SetKey(), oldKey)
	// 添加新策略
	pipe.Set(ctx, newKey, newLine, a.ttl)
	pipe.SAdd(ctx, a.keys.SetKey(), newKey)
	if ptype := policy.ExtractPType(newLine); ptype != "" {
		pipe.SAdd(ctx, a.keys.TypeKey(), ptype)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyUpdateFailedError(err.Error())
	}

	a.logger.DebugKV("Policy updated in Redis", "old", oldLine, "new", newLine)
	return nil
}

// UpdatePoliciesWithCtx 批量更新策略
// 要求旧策略和新策略数量必须一致
func (a *Adapter) UpdatePoliciesWithCtx(ctx context.Context, oldLines, newLines []string) error {
	if len(oldLines) != len(newLines) {
		return errors.NewPolicyCountMismatchError("old and new policy counts must match")
	}

	ctx = contextx.OrBackground(ctx)
	pipe := a.client.Pipeline()

	for i, oldLine := range oldLines {
		oldKey := a.keys.PolicyKey(oldLine)
		newKey := a.keys.PolicyKey(newLines[i])

		// 删除旧策略
		pipe.Del(ctx, oldKey)
		pipe.SRem(ctx, a.keys.SetKey(), oldKey)
		// 添加新策略
		pipe.Set(ctx, newKey, newLines[i], a.ttl)
		pipe.SAdd(ctx, a.keys.SetKey(), newKey)
		if ptype := policy.ExtractPType(newLines[i]); ptype != "" {
			pipe.SAdd(ctx, a.keys.TypeKey(), ptype)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.NewPolicyBatchUpdateFailedError(err.Error())
	}

	return nil
}

// UpdateFilteredPoliciesWithCtx 根据字段索引过滤后更新策略
// 优先从 newLines 推断单一 ptype；无法推断时按历史兼容语义更新所有 ptype 中匹配的策略
func (a *Adapter) UpdateFilteredPoliciesWithCtx(ctx context.Context, newLines []string, fieldIndex int, fieldValues ...string) error {
	if ptype := policy.InferPType(newLines); ptype != "" {
		return a.UpdateFilteredPoliciesByPTypeWithCtx(ctx, ptype, newLines, fieldIndex, fieldValues...)
	}
	return a.replaceFilteredPoliciesWithLua(ctx, "", newLines, fieldIndex, fieldValues...)
}

// UpdateFilteredPoliciesByPTypeWithCtx 根据策略类型（p/g）和字段索引过滤后更新策略
// 删除旧策略与插入新策略由 Redis Lua 脚本原子完成，避免更新 g 时误删 p
func (a *Adapter) UpdateFilteredPoliciesByPTypeWithCtx(ctx context.Context, ptype string, newLines []string, fieldIndex int, fieldValues ...string) error {
	ptype = strings.TrimSpace(ptype)
	if ptype == "" {
		return a.UpdateFilteredPoliciesWithCtx(ctx, newLines, fieldIndex, fieldValues...)
	}
	return a.replaceFilteredPoliciesWithLua(ctx, ptype, policy.FilterPoliciesByPType(newLines, ptype), fieldIndex, fieldValues...)
}

// LoadFilteredPolicyWithCtx 根据过滤条件从 Redis 加载策略
// 支持 policy.Filter 和 []string 两种过滤格式
func (a *Adapter) LoadFilteredPolicyWithCtx(ctx context.Context, filter interface{}) ([]string, error) {
	a.filtered = true

	// 先加载所有策略
	policies, err := a.LoadPolicyWithCtx(ctx)
	if err != nil {
		return nil, err
	}

	// 根据过滤条件筛选
	var result []string
	switch f := filter.(type) {
	case *policy.Filter:
		result = policy.FilterPolicies(policies, f)
	case []string:
		result = policy.FilterPolicies(policies, policy.FilterFromSlice(f))
	default:
		result = policies
	}

	a.logger.InfoKV("Filtered policies loaded from Redis", "count", len(result))
	return result, nil
}

// RemoveFilteredPolicyWithCtx 根据字段索引和值删除匹配的策略
// 通过 Lua 在 Redis 侧完成过滤和删除，fieldIndex=0 表示 V0
func (a *Adapter) RemoveFilteredPolicyWithCtx(ctx context.Context, fieldIndex int, fieldValues ...string) error {
	if err := a.replaceFilteredPoliciesWithLua(ctx, "", nil, fieldIndex, fieldValues...); err != nil {
		return err
	}

	a.logger.DebugKV("Filtered policies removed from Redis", "field_index", fieldIndex)
	return nil
}

// GetPolicyByPTypeWithCtx 根据策略类型（p/g）从 Redis 加载策略
func (a *Adapter) GetPolicyByPTypeWithCtx(ctx context.Context, ptype string) ([]string, error) {
	policies, err := a.LoadPolicyWithCtx(ctx)
	if err != nil {
		return nil, errors.NewPolicyGetByTypeFailedError(err.Error())
	}

	result := make([]string, 0)
	for _, p := range policies {
		if policy.ExtractPType(p) == ptype {
			result = append(result, p)
		}
	}

	return result, nil
}

// ==================== 非 ctx 方法（包装 WithCtx） ====================
// 非 ctx 方法内部调用对应的 WithCtx 方法，使用 context.Background() 作为默认上下文
// 用户可以直接调用 WithCtx 方法传入自定义 context 实现超时控制和链路追踪

// LoadPolicy 从 Redis 加载所有策略（无上下文）
func (a *Adapter) LoadPolicy() ([]string, error) {
	return a.LoadPolicyWithCtx(context.Background())
}

// SavePolicy 保存所有策略到 Redis（无上下文）
func (a *Adapter) SavePolicy(policies []string) error {
	return a.SavePolicyWithCtx(context.Background(), policies)
}

// AddPolicy 添加单条策略（无上下文）
func (a *Adapter) AddPolicy(line string) error {
	return a.AddPolicyWithCtx(context.Background(), line)
}

// RemovePolicy 删除单条策略（无上下文）
func (a *Adapter) RemovePolicy(line string) error {
	return a.RemovePolicyWithCtx(context.Background(), line)
}

// AddPolicies 批量添加策略（无上下文）
func (a *Adapter) AddPolicies(lines []string) error {
	return a.AddPoliciesWithCtx(context.Background(), lines)
}

// RemovePolicies 批量删除策略（无上下文）
func (a *Adapter) RemovePolicies(lines []string) error {
	return a.RemovePoliciesWithCtx(context.Background(), lines)
}

// UpdatePolicy 更新单条策略（无上下文）
func (a *Adapter) UpdatePolicy(oldLine, newLine string) error {
	return a.UpdatePolicyWithCtx(context.Background(), oldLine, newLine)
}

// UpdatePolicies 批量更新策略（无上下文）
func (a *Adapter) UpdatePolicies(oldLines, newLines []string) error {
	return a.UpdatePoliciesWithCtx(context.Background(), oldLines, newLines)
}

// UpdateFilteredPolicies 根据字段索引过滤后更新策略（无上下文）
func (a *Adapter) UpdateFilteredPolicies(newLines []string, fieldIndex int, fieldValues ...string) error {
	return a.UpdateFilteredPoliciesWithCtx(context.Background(), newLines, fieldIndex, fieldValues...)
}

// UpdateFilteredPoliciesByPType 根据策略类型（p/g）过滤后更新策略（无上下文）
func (a *Adapter) UpdateFilteredPoliciesByPType(ptype string, newLines []string, fieldIndex int, fieldValues ...string) error {
	return a.UpdateFilteredPoliciesByPTypeWithCtx(context.Background(), ptype, newLines, fieldIndex, fieldValues...)
}

// LoadFilteredPolicy 根据过滤条件加载策略（无上下文）
func (a *Adapter) LoadFilteredPolicy(filter interface{}) ([]string, error) {
	return a.LoadFilteredPolicyWithCtx(context.Background(), filter)
}

// IsFiltered 返回是否已使用过滤加载
func (a *Adapter) IsFiltered() bool {
	return a.filtered
}

// RemoveFilteredPolicy 根据字段索引和值删除匹配的策略（无上下文）
func (a *Adapter) RemoveFilteredPolicy(fieldIndex int, fieldValues ...string) error {
	return a.RemoveFilteredPolicyWithCtx(context.Background(), fieldIndex, fieldValues...)
}

// GetPolicyByPType 根据策略类型加载策略（无上下文）
func (a *Adapter) GetPolicyByPType(ptype string) ([]string, error) {
	return a.GetPolicyByPTypeWithCtx(context.Background(), ptype)
}

// ==================== 辅助方法 ====================

// Close 关闭 Redis 适配器
// 同时关闭 go-cachex 处理器和 Redis 客户端连接
func (a *Adapter) Close() error {
	if a.handler != nil {
		a.handler.Close()
	}
	return a.client.Close()
}

// GetClient 获取底层 Redis 客户端
// 可用于执行自定义 Redis 命令
func (a *Adapter) GetClient() *redis.Client {
	return a.client
}

// replaceFilteredPoliciesWithLua 使用 Lua 原子替换过滤后的策略
// ptype 为空时保持旧接口兼容：跨所有 ptype 匹配；非空时只替换指定 ptype
func (a *Adapter) replaceFilteredPoliciesWithLua(ctx context.Context, ptype string, newLines []string, fieldIndex int, fieldValues ...string) error {
	ctx = contextx.OrBackground(ctx)

	ttlMS := int64(0)
	if a.ttl > 0 {
		ttlMS = a.ttl.Milliseconds()
		if ttlMS == 0 {
			ttlMS = 1
		}
	}

	args := make([]interface{}, 0, 6+len(fieldValues)+len(newLines))
	args = append(args, a.keys.prefix, ttlMS, strings.TrimSpace(ptype), fieldIndex, len(fieldValues))
	for _, value := range fieldValues {
		args = append(args, value)
	}
	args = append(args, len(newLines))
	for _, line := range newLines {
		args = append(args, line)
	}

	if err := replaceFilteredPoliciesScript.Run(ctx, a.client, []string{a.keys.SetKey(), a.keys.TypeKey()}, args...).Err(); err != nil {
		return errors.NewPolicyBatchUpdateFailedError(err.Error())
	}
	return nil
}

// clearAll 清空 Redis 中所有策略数据
// 删除所有策略 Key、策略集合和类型集合
func (a *Adapter) clearAll(ctx context.Context) error {
	keys, err := a.client.SMembers(ctx, a.keys.SetKey()).Result()
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	pipe := a.client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}
	pipe.Del(ctx, a.keys.SetKey())
	pipe.Del(ctx, a.keys.TypeKey())

	_, err = pipe.Exec(ctx)
	return err
}
