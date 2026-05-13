package session_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/session"
)

func TestCreateSession(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	if sess.ID == "" {
		t.Fatal("expected non-empty session ID")
	}
}

func TestAddAndGetRules(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	rule := session.Rule{ID: "r1", Protocol: "http"}
	store.AddRule(sess.ID, rule)
	rules := store.Rules(sess.ID)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].ID != "r1" {
		t.Fatalf("expected rule ID r1, got %s", rules[0].ID)
	}
}

func TestDeleteRule(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	store.AddRule(sess.ID, session.Rule{ID: "r1", Protocol: "http"})
	store.DeleteRule(sess.ID, "r1")
	if len(store.Rules(sess.ID)) != 0 {
		t.Fatal("expected 0 rules after delete")
	}
}

func TestDeleteSession(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	store.Delete(sess.ID)
	if store.Get(sess.ID) != nil {
		t.Fatal("expected nil after session delete")
	}
}

func TestGetNonexistent(t *testing.T) {
	store := session.NewStore()
	if store.Get("no-such-id") != nil {
		t.Fatal("expected nil for nonexistent session")
	}
}

func TestRuleByBackendID(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	store.AddRule(sess.ID, session.Rule{ID: "r1", BackendID: "wm-123", Protocol: "http"})
	rule := store.RuleByBackendID(sess.ID, "wm-123")
	if rule == nil || rule.ID != "r1" {
		t.Fatal("expected to find rule by backend ID")
	}
}
