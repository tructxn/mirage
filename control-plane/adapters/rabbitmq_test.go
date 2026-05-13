package adapters_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestRabbitMQPushRuleMissingExchange(t *testing.T) {
	adapter := adapters.NewRabbitMQAdapter("amqp://guest:guest@localhost:15673/")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "amqp",
		Match:    map[string]any{}, // no exchange
		Response: "test-message",
	}
	if err := adapter.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing exchange")
	}
}

func TestRabbitMQHealthyUnreachable(t *testing.T) {
	adapter := adapters.NewRabbitMQAdapter("amqp://guest:guest@localhost:19999/")
	if adapter.Healthy() {
		t.Fatal("expected Healthy()=false")
	}
}
