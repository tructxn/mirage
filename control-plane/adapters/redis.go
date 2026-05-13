package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tructxn/mirage/control-plane/session"
)

type RedisAdapter struct {
	addr string
	rdb  *redis.Client
}

func NewRedisAdapter(addr string) *RedisAdapter {
	rdb := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 3 * time.Second,
	})
	return &RedisAdapter{addr: addr, rdb: rdb}
}

// PushRule SETs the key with the configured response value.
// Supports exact key matching only. Pattern matching is Phase 4.
func (a *RedisAdapter) PushRule(sessionID string, rule *session.Rule) error {
	key, ok := rule.Match["key"].(string)
	if !ok || key == "" {
		return fmt.Errorf("redis rule missing match.key")
	}
	val := fmt.Sprintf("%v", rule.Response)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return a.rdb.Set(ctx, key, val, 0).Err()
}

func (a *RedisAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	key, ok := rule.Match["key"].(string)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return a.rdb.Del(ctx, key).Err()
}

func (a *RedisAdapter) Reset(sessionID string, rules []session.Rule) error {
	for _, r := range rules {
		_ = a.DeleteRule(sessionID, r)
	}
	return nil
}

// Traffic is not natively supported — returns empty. Phase 4 adds recording.
func (a *RedisAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil
}

func (a *RedisAdapter) Healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return a.rdb.Ping(ctx).Err() == nil
}
