package adapters_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestKafkaPushRuleMissingTopic(t *testing.T) {
	adapter := adapters.NewKafkaAdapter("localhost:19999")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "kafka",
		Match:    map[string]any{},
		Response: "some-message",
	}
	if err := adapter.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestKafkaHealthyUnreachable(t *testing.T) {
	if adapters.NewKafkaAdapter("localhost:19999").Healthy() {
		t.Fatal("expected Healthy()=false for unreachable broker")
	}
}

func TestKafkaDeleteRuleIsNoop(t *testing.T) {
	adapter := adapters.NewKafkaAdapter("localhost:19999")
	err := adapter.DeleteRule("sess-1", session.Rule{ID: "r1"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
