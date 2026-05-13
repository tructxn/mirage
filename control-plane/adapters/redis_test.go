package adapters_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestRedisPushRule(t *testing.T) {
	mr := miniredis.RunT(t)
	adapter := adapters.NewRedisAdapter(mr.Addr())
	rule := &session.Rule{
		ID:       "rule-1",
		Protocol: "redis",
		Match:    map[string]any{"command": "GET", "key": "user:42"},
		Response: `{"id":42}`,
	}
	if err := adapter.PushRule("sess-1", rule); err != nil {
		t.Fatalf("PushRule failed: %v", err)
	}
	got, err := mr.Get("user:42")
	if err != nil {
		t.Fatalf("key user:42 not found in Redis: %v", err)
	}
	if got != `{"id":42}` {
		t.Fatalf("expected {\"id\":42}, got %q", got)
	}
}

func TestRedisPushRuleMissingKey(t *testing.T) {
	mr := miniredis.RunT(t)
	adapter := adapters.NewRedisAdapter(mr.Addr())
	rule := &session.Rule{
		ID:       "rule-1",
		Protocol: "redis",
		Match:    map[string]any{},
		Response: "value",
	}
	if err := adapter.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestRedisReset(t *testing.T) {
	mr := miniredis.RunT(t)
	adapter := adapters.NewRedisAdapter(mr.Addr())
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "redis",
		Match:    map[string]any{"command": "GET", "key": "some-key"},
		Response: "some-value",
	}
	_ = adapter.PushRule("sess-1", rule)
	_ = adapter.Reset("sess-1", []session.Rule{*rule})
	_, err := mr.Get("some-key")
	if err == nil {
		t.Fatal("expected key to be deleted after reset")
	}
}

func TestRedisHealthy(t *testing.T) {
	mr := miniredis.RunT(t)
	if !adapters.NewRedisAdapter(mr.Addr()).Healthy() {
		t.Fatal("expected Healthy()=true")
	}
}

func TestRedisHealthyUnreachable(t *testing.T) {
	if adapters.NewRedisAdapter("localhost:19999").Healthy() {
		t.Fatal("expected Healthy()=false for unreachable redis")
	}
}
