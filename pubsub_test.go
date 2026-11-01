/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-05-26 19:51:02
 * @FilePath: \go-casbin-redis-adapter\pubsub_test.go
 * @Description: 测试 RedisNotifier 的发布订阅功能
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */
package redisadapter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/redis/go-redis/v9"
)

func newTestNotifier(t *testing.T, source string) (*RedisNotifier, *redis.Client) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	notifier, err := NewRedisNotifier(
		client,
		policy.WithSource(source),
		policy.WithChannel("test:policy"),
		policy.WithRetry(time.Millisecond, 1),
	)
	if err != nil {
		t.Fatalf("NewRedisNotifier() error = %v", err)
	}
	return notifier, client
}

func TestRedisNotifierValidationAndLifecycle(t *testing.T) {
	if _, err := NewRedisNotifier(nil); err == nil {
		t.Fatal("NewRedisNotifier(nil) expected error")
	}

	notifier, _ := newTestNotifier(t, "node-a")
	WithNotifierLogger(logger.NewEmptyLogger())(notifier)
	WithNotifierLogger(nil)(notifier)

	if err := notifier.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe(not running) error = %v", err)
	}
	if err := notifier.Subscribe(context.Background(), nil); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := notifier.Subscribe(context.Background(), nil); err != nil {
		t.Fatalf("Subscribe(already running) error = %v", err)
	}
	if err := notifier.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRedisNotifierPublishHelpers(t *testing.T) {
	notifier, _ := newTestNotifier(t, "node-a")

	if err := notifier.PublishPolicyAdded(context.Background(), "p", []string{"alice"}); err != nil {
		t.Fatalf("PublishPolicyAdded() error = %v", err)
	}
	if err := notifier.PublishPolicyRemoved(context.Background(), "p", []string{"alice"}); err != nil {
		t.Fatalf("PublishPolicyRemoved() error = %v", err)
	}
	if err := notifier.PublishPolicyUpdated(context.Background(), "p", []string{"old"}, []string{"new"}); err != nil {
		t.Fatalf("PublishPolicyUpdated() error = %v", err)
	}
	if err := notifier.PublishPolicyReload(context.Background()); err != nil {
		t.Fatalf("PublishPolicyReload() error = %v", err)
	}
}

// waitSubscribed 等到服务端真的注册了订阅者再往下走
//
// go-redis 的 Subscribe 是惰性的：SUBSCRIBE 命令在首次使用时才发出，所以
// notifier.Subscribe 返回**不代表**服务端已经注册。订阅没就绪时发出的消息会被直接丢掉，
// 测试于是卡满超时再失败 —— 共享 CI runner 上比本地更容易撞上，这正是它在 CI 上红的原因
//
// miniredis 暴露了 PubSubNumSub，所以这里等的是「订阅者数 > 0」这个确定的事实，
// 而不是 sleep 一个猜出来的时长
//
// [EN] go-redis subscribes lazily: the SUBSCRIBE command goes out on first use, so Subscribe
// returning does not mean the server has registered it, and a message published before then is
// dropped. miniredis exposes PubSubNumSub, so this waits on the actual fact rather than sleeping
// for a guessed duration
func waitSubscribed(t *testing.T, server *miniredis.Miniredis, channel string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if server.PubSubNumSub(channel)[channel] > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等了 3 秒，服务端仍未注册频道 %q 的订阅者", channel)
}

func TestRedisNotifierSubscribeReceivesOtherSource(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})

	sub, err := NewRedisNotifier(clientA, policy.WithSource("node-a"), policy.WithChannel("test:policy"))
	if err != nil {
		t.Fatalf("NewRedisNotifier(sub) error = %v", err)
	}
	pub, err := NewRedisNotifier(clientB, policy.WithSource("node-b"), policy.WithChannel("test:policy"))
	if err != nil {
		t.Fatalf("NewRedisNotifier(pub) error = %v", err)
	}

	received := make(chan *policy.ChangeEvent, 1)
	if err := sub.Subscribe(context.Background(), func(event *policy.ChangeEvent) {
		received <- event
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer func() {
		_ = sub.Close()
	}()

	// 发布之前先确认订阅已在服务端就绪，否则消息会丢
	waitSubscribed(t, server, "test:policy")

	if err := pub.PublishPolicyAdded(context.Background(), "p", []string{"alice"}); err != nil {
		t.Fatalf("PublishPolicyAdded() error = %v", err)
	}

	select {
	case event := <-received:
		if event.Type != policy.EventTypePolicyAdded || event.Source != "node-b" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}
