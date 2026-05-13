package adapters_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestMySQLPushRuleMissingTable(t *testing.T) {
	adapter := adapters.NewMySQLAdapter("root:@tcp(localhost:19999)/mirage")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "mysql",
		Match:    map[string]any{},
		Response: []any{},
	}
	if err := adapter.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing table")
	}
}

func TestMySQLHealthyUnreachable(t *testing.T) {
	adapter := adapters.NewMySQLAdapter("root:@tcp(localhost:19999)/mirage")
	if adapter.Healthy() {
		t.Fatal("expected Healthy()=false for unreachable MySQL")
	}
}
