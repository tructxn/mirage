// control-plane/adapters/rabbitmq_rpc_test.go
package adapters

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

// helpers
func rpcRules(rs ...session.Rule) []session.Rule { return rs }

func rpcRule(id string, priority int, match map[string]any) session.Rule {
	return session.Rule{ID: id, Protocol: "amqp-rpc", Match: match, Response: `{"ok":true}`, Priority: priority}
}

func rpcDelivery(typ, contentType string, headers amqp.Table) amqp.Delivery {
	return amqp.Delivery{
		Type:          typ,
		ContentType:   contentType,
		Headers:       headers,
		ReplyTo:       "amq.rabbitmq.reply-to",
		CorrelationId: "corr-123",
	}
}

// matchRule tests — pure function, no network

func TestMatchRuleByQueue(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "profile_service"})
	d := rpcDelivery("", "", nil)
	got, ok := matchRule(rpcRules(r), d)
	if !ok || got.ID != "r1" {
		t.Fatalf("expected match r1, got ok=%v id=%s", ok, got.ID)
	}
}

func TestMatchRuleByPropertyType(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := rpcDelivery("GET_PROFILE", "", nil)
	_, ok := matchRule(rpcRules(r), d)
	if !ok {
		t.Fatal("expected match on properties.type")
	}
}

func TestMatchRuleByPropertyTypeMiss(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := rpcDelivery("OTHER_TYPE", "", nil)
	_, ok := matchRule(rpcRules(r), d)
	if ok {
		t.Fatal("expected no match when properties.type differs")
	}
}

func TestMatchRuleByHeader(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "q", "headers.X-Source": "payment-service"})
	d := rpcDelivery("", "", amqp.Table{"X-Source": "payment-service"})
	_, ok := matchRule(rpcRules(r), d)
	if !ok {
		t.Fatal("expected match on header")
	}
}

func TestMatchRuleByHeaderMiss(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "q", "headers.X-Source": "payment-service"})
	d := rpcDelivery("", "", amqp.Table{"X-Source": "other-service"})
	_, ok := matchRule(rpcRules(r), d)
	if ok {
		t.Fatal("expected no match when header value differs")
	}
}

func TestMatchRuleAllFilters(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{
		"queue":            "q",
		"properties.type":  "GET_PROFILE",
		"headers.X-Source": "payment-service",
	})
	d := rpcDelivery("GET_PROFILE", "", amqp.Table{"X-Source": "payment-service"})
	_, ok := matchRule(rpcRules(r), d)
	if !ok {
		t.Fatal("expected match when all filters satisfied")
	}
	d2 := rpcDelivery("GET_PROFILE", "", amqp.Table{"X-Source": "wrong"})
	_, ok2 := matchRule(rpcRules(r), d2)
	if ok2 {
		t.Fatal("expected no match when one filter fails")
	}
}

func TestMatchRulePriority(t *testing.T) {
	low := rpcRule("low", 1, map[string]any{"queue": "q"})
	high := rpcRule("high", 10, map[string]any{"queue": "q"})
	d := rpcDelivery("", "", nil)
	// matchRule is a pure scan; caller must pre-sort by priority descending.
	got, ok := matchRule(rpcRules(high, low), d)
	if !ok || got.ID != "high" {
		t.Fatalf("expected high-priority rule to win, got id=%s ok=%v", got.ID, ok)
	}
}

func TestMatchRuleNoMatch(t *testing.T) {
	r := rpcRule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := rpcDelivery("OTHER", "", nil)
	_, ok := matchRule(rpcRules(r), d)
	if ok {
		t.Fatal("expected no match")
	}
}

func TestMatchRuleEmptyRules(t *testing.T) {
	d := rpcDelivery("GET_PROFILE", "", nil)
	_, ok := matchRule(nil, d)
	if ok {
		t.Fatal("expected no match for empty rules")
	}
}

// PushRule validation

func TestRPCPushRuleMissingQueue(t *testing.T) {
	a := NewRabbitMQRPCAdapter("amqp://guest:guest@localhost:19999/")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "amqp-rpc",
		Match:    map[string]any{},
		Response: `{"ok":true}`,
	}
	if err := a.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing queue")
	}
}

func TestRPCHealthyUnreachable(t *testing.T) {
	a := NewRabbitMQRPCAdapter("amqp://guest:guest@localhost:19999/")
	if a.Healthy() {
		t.Fatal("expected Healthy()=false for unreachable broker")
	}
}
