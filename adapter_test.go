/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-05-26 19:38:15
 * @FilePath: \go-casbin-redis-adapter\adapter_test.go
 * @Description: Casbin Redis 适配器 - 基于 Redis 的策略分布式缓存存储 测试
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */
package redisadapter

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/redis/go-redis/v9"
)

func newTestAdapter(t *testing.T) *Adapter {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	adapter, err := NewAdapter(client, WithKeyPrefix("test:casbin:"))
	if err != nil {
		t.Fatalf("NewAdapter() error = %v", err)
	}
	return adapter
}

func loadSortedPolicies(t *testing.T, adapter *Adapter) []string {
	t.Helper()

	policies, err := adapter.LoadPolicy()
	if err != nil {
		t.Fatalf("LoadPolicy() error = %v", err)
	}
	sort.Strings(policies)
	return policies
}

func TestUpdateFilteredPoliciesByPType_RemovesOnlyGroupingRules(t *testing.T) {
	adapter := newTestAdapter(t)
	if err := adapter.SavePolicy([]string{
		"p, alice, ops, /v1/users, GET",
		"g, alice, role:admin, ops",
	}); err != nil {
		t.Fatalf("SavePolicy() error = %v", err)
	}

	err := adapter.UpdateFilteredPoliciesByPType("g", nil, 0, "alice")
	if err != nil {
		t.Fatalf("UpdateFilteredPoliciesByPType() error = %v", err)
	}

	want := []string{
		"p, alice, ops, /v1/users, GET",
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}
}

func TestUpdateFilteredPoliciesByPType_ReplacesOnlyPolicyRules(t *testing.T) {
	adapter := newTestAdapter(t)
	if err := adapter.SavePolicy([]string{
		"p, role:admin, ops, /v1/users, GET",
		"g, role:admin, role:parent, ops",
	}); err != nil {
		t.Fatalf("SavePolicy() error = %v", err)
	}

	err := adapter.UpdateFilteredPoliciesByPType("p", []string{
		"p, role:admin, ops, /v1/users, POST",
		"g, role:admin, role:ignored, ops",
	}, 0, "role:admin")
	if err != nil {
		t.Fatalf("UpdateFilteredPoliciesByPType() error = %v", err)
	}

	want := []string{
		"g, role:admin, role:parent, ops",
		"p, role:admin, ops, /v1/users, POST",
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}
}

func TestUpdateFilteredPoliciesInfersPType(t *testing.T) {
	adapter := newTestAdapter(t)
	if err := adapter.SavePolicy([]string{
		"p, role:admin, ops, /v1/users, GET",
		"g, role:admin, role:parent, ops",
	}); err != nil {
		t.Fatalf("SavePolicy() error = %v", err)
	}

	err := adapter.UpdateFilteredPolicies([]string{
		"g, role:admin, role:next, ops",
	}, 0, "role:admin")
	if err != nil {
		t.Fatalf("UpdateFilteredPolicies() error = %v", err)
	}

	want := []string{
		"g, role:admin, role:next, ops",
		"p, role:admin, ops, /v1/users, GET",
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNewAdapterValidationAndOptions(t *testing.T) {
	if _, err := NewAdapter(nil); err == nil {
		t.Fatal("NewAdapter(nil) expected error")
	}
	if _, err := NewAdapterWithConfig(nil); err == nil {
		t.Fatal("NewAdapterWithConfig(nil) expected error")
	}
	if _, err := NewAdapterWithConfig(&redis.Options{}); err == nil {
		t.Fatal("NewAdapterWithConfig(empty addr) expected error")
	}

	server := miniredis.RunT(t)
	adapter, err := NewAdapterWithConfig(
		&redis.Options{Addr: server.Addr()},
		WithKeyPrefix("custom:"),
		WithTTL(time.Minute),
		WithLogger(logger.NewEmptyLogger()),
	)
	if err != nil {
		t.Fatalf("NewAdapterWithConfig() error = %v", err)
	}
	if adapter.keys.SetKey() != "custom:set" {
		t.Fatalf("SetKey() = %q", adapter.keys.SetKey())
	}
	if adapter.ttl != time.Minute {
		t.Fatalf("ttl = %v", adapter.ttl)
	}
	if adapter.GetClient() == nil {
		t.Fatal("GetClient() returned nil")
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestAdapterCRUDAndQueries(t *testing.T) {
	adapter := newTestAdapter(t)

	if err := adapter.AddPolicy("p, alice, ops, /v1/users, GET"); err != nil {
		t.Fatalf("AddPolicy() error = %v", err)
	}
	if err := adapter.AddPolicyWithCtx(context.Background(), "g, alice, role:admin, ops"); err != nil {
		t.Fatalf("AddPolicyWithCtx() error = %v", err)
	}
	if err := adapter.AddPolicies([]string{
		"p, bob, ops, /v1/users, GET",
		"p, carol, ops, /v1/users, POST",
	}); err != nil {
		t.Fatalf("AddPolicies() error = %v", err)
	}

	want := []string{
		"g, alice, role:admin, ops",
		"p, alice, ops, /v1/users, GET",
		"p, bob, ops, /v1/users, GET",
		"p, carol, ops, /v1/users, POST",
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}

	filtered, err := adapter.LoadFilteredPolicy(policy.NewFilter().WithPType("p").WithV0("alice"))
	if err != nil {
		t.Fatalf("LoadFilteredPolicy() error = %v", err)
	}
	if !adapter.IsFiltered() {
		t.Fatal("IsFiltered() = false, want true")
	}
	if !equalStringSlices(filtered, []string{"p, alice, ops, /v1/users, GET"}) {
		t.Fatalf("filtered = %v", filtered)
	}

	filtered, err = adapter.LoadFilteredPolicy([]string{"g", "alice"})
	if err != nil {
		t.Fatalf("LoadFilteredPolicy([]string) error = %v", err)
	}
	if !equalStringSlices(filtered, []string{"g, alice, role:admin, ops"}) {
		t.Fatalf("filtered = %v", filtered)
	}

	filtered, err = adapter.LoadFilteredPolicy("unsupported")
	if err != nil {
		t.Fatalf("LoadFilteredPolicy(default) error = %v", err)
	}
	if len(filtered) != 4 {
		t.Fatalf("default filtered count = %d, want 4", len(filtered))
	}

	pPolicies, err := adapter.GetPolicyByPType("p")
	if err != nil {
		t.Fatalf("GetPolicyByPType() error = %v", err)
	}
	if len(pPolicies) != 3 {
		t.Fatalf("p policy count = %d, want 3", len(pPolicies))
	}

	if err := adapter.UpdatePolicy("p, bob, ops, /v1/users, GET", "p, bob, ops, /v1/users, PUT"); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	if err := adapter.UpdatePolicies(
		[]string{"p, carol, ops, /v1/users, POST"},
		[]string{"p, carol, ops, /v1/users, DELETE"},
	); err != nil {
		t.Fatalf("UpdatePolicies() error = %v", err)
	}
	if err := adapter.UpdatePolicies([]string{"p, one"}, []string{"p, one", "p, two"}); err == nil {
		t.Fatal("UpdatePolicies() mismatch expected error")
	}

	if err := adapter.RemovePolicy("p, alice, ops, /v1/users, GET"); err != nil {
		t.Fatalf("RemovePolicy() error = %v", err)
	}
	if err := adapter.RemovePolicies([]string{"p, bob, ops, /v1/users, PUT"}); err != nil {
		t.Fatalf("RemovePolicies() error = %v", err)
	}
	if err := adapter.RemoveFilteredPolicy(0, "carol"); err != nil {
		t.Fatalf("RemoveFilteredPolicy() error = %v", err)
	}

	want = []string{"g, alice, role:admin, ops"}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}
}

func TestAdapterSavePolicyEmptyAndTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	adapter, err := NewAdapter(client, WithTTL(time.Second))
	if err != nil {
		t.Fatalf("NewAdapter() error = %v", err)
	}
	if err := adapter.SavePolicy(nil); err != nil {
		t.Fatalf("SavePolicy(nil) error = %v", err)
	}
	if got := loadSortedPolicies(t, adapter); len(got) != 0 {
		t.Fatalf("policies = %v, want empty", got)
	}
	if err := adapter.AddPolicy("p, ttl, ops, /v1/ttl, GET"); err != nil {
		t.Fatalf("AddPolicy() error = %v", err)
	}
	if err := adapter.SavePolicy([]string{"p, reset, ops, /v1/reset, GET"}); err != nil {
		t.Fatalf("SavePolicy(replace) error = %v", err)
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, []string{"p, reset, ops, /v1/reset, GET"}) {
		t.Fatalf("replaced policies = %v", got)
	}
	server.FastForward(2 * time.Second)
	if got := loadSortedPolicies(t, adapter); len(got) != 0 {
		t.Fatalf("expired policies = %v, want empty", got)
	}
}

func TestAdapterMixedPTypeLegacyUpdateAndNoMatchRemove(t *testing.T) {
	adapter := newTestAdapter(t)
	if err := adapter.SavePolicy([]string{
		"p, alice, ops, /v1/users, GET",
		"g, alice, role:admin, ops",
		"p, bob, ops, /v1/users, GET",
	}); err != nil {
		t.Fatalf("SavePolicy() error = %v", err)
	}

	if err := adapter.UpdateFilteredPolicies([]string{
		"p, alice, ops, /v1/users, POST",
		"g, alice, role:next, ops",
	}, 0, "alice"); err != nil {
		t.Fatalf("UpdateFilteredPolicies(mixed) error = %v", err)
	}
	want := []string{
		"g, alice, role:next, ops",
		"p, alice, ops, /v1/users, POST",
		"p, bob, ops, /v1/users, GET",
	}
	if got := loadSortedPolicies(t, adapter); !equalStringSlices(got, want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}

	if err := adapter.RemoveFilteredPolicy(0, "missing"); err != nil {
		t.Fatalf("RemoveFilteredPolicy(no match) error = %v", err)
	}
	if err := adapter.UpdateFilteredPoliciesByPType("", nil, 0, "missing"); err != nil {
		t.Fatalf("UpdateFilteredPoliciesByPType(empty ptype) error = %v", err)
	}
	if err := adapter.AddPolicies(nil); err != nil {
		t.Fatalf("AddPolicies(nil) error = %v", err)
	}
	if err := adapter.RemovePolicies(nil); err != nil {
		t.Fatalf("RemovePolicies(nil) error = %v", err)
	}
}

func TestKeyBuilderDefaultsAndExtractPType(t *testing.T) {
	keys := NewKeyBuilder("")
	if keys.SetKey() != DefaultKeyPrefix+"set" {
		t.Fatalf("default SetKey() = %q", keys.SetKey())
	}
	if got := ExtractPType("g, alice, role:admin"); got != "g" {
		t.Fatalf("ExtractPType() = %q, want g", got)
	}
	if got := ExtractPType(""); got != "" {
		t.Fatalf("ExtractPType(empty) = %q, want empty", got)
	}
}
