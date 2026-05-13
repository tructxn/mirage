package adapters_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestWireMockPushRule(t *testing.T) {
	var received map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/__admin/mappings" {
			json.NewDecoder(r.Body).Decode(&received)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"id": "wm-stub-id"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	adapter := adapters.NewWireMockAdapter(ts.URL)
	rule := &session.Rule{
		ID:       "rule-1",
		Protocol: "http",
		Match:    map[string]any{"method": "GET", "path": "/ping"},
		Response: map[string]any{"status": float64(200), "body": "pong"},
	}
	if err := adapter.PushRule("sess-1", rule); err != nil {
		t.Fatalf("PushRule failed: %v", err)
	}
	if rule.BackendID != "wm-stub-id" {
		t.Fatalf("expected BackendID=wm-stub-id, got %q", rule.BackendID)
	}
	if received["request"] == nil {
		t.Fatal("expected request field in WireMock payload")
	}
}

func TestWireMockHealthy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__admin/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()
	if !adapters.NewWireMockAdapter(ts.URL).Healthy() {
		t.Fatal("expected Healthy()=true")
	}
}

func TestWireMockHealthyUnreachable(t *testing.T) {
	if adapters.NewWireMockAdapter("http://localhost:19999").Healthy() {
		t.Fatal("expected Healthy()=false for unreachable server")
	}
}
