package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestCreateMock(t *testing.T) {
	store := session.NewStore()
	srv := api.NewServer(store, adapters.NewRegistry())
	sessID := mustCreateSession(t, srv)

	body := `{"protocol":"http","match":{"method":"GET","path":"/ping"},"response":{"status":200,"body":"pong"}}`
	req := httptest.NewRequest(http.MethodPost, "/sessions/"+sessID+"/mocks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["id"] == "" {
		t.Fatal("expected rule id in response")
	}
}

func TestListMocks(t *testing.T) {
	store := session.NewStore()
	srv := api.NewServer(store, adapters.NewRegistry())
	sessID := mustCreateSession(t, srv)

	body := `{"protocol":"http","match":{"method":"GET","path":"/ping"},"response":{"status":200,"body":"pong"}}`
	req := httptest.NewRequest(http.MethodPost, "/sessions/"+sessID+"/mocks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(httptest.NewRecorder(), req)

	req2 := httptest.NewRequest(http.MethodGet, "/sessions/"+sessID+"/mocks", nil)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}
	var rules []session.Rule
	json.NewDecoder(w2.Body).Decode(&rules)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
}

func TestDeleteMock(t *testing.T) {
	store := session.NewStore()
	srv := api.NewServer(store, adapters.NewRegistry())
	sessID := mustCreateSession(t, srv)

	body := `{"protocol":"http","match":{"method":"GET","path":"/ping"},"response":{"status":200,"body":"pong"}}`
	req := httptest.NewRequest(http.MethodPost, "/sessions/"+sessID+"/mocks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	wr := httptest.NewRecorder()
	srv.ServeHTTP(wr, req)

	var created map[string]string
	json.NewDecoder(wr.Body).Decode(&created)
	ruleID := created["id"]

	req2 := httptest.NewRequest(http.MethodDelete, "/sessions/"+sessID+"/mocks/"+ruleID, nil)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req2)
	if w2.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w2.Code)
	}
}

func TestCreateMockSessionNotFound(t *testing.T) {
	srv := api.NewServer(session.NewStore(), adapters.NewRegistry())
	body := `{"protocol":"http","match":{},"response":{}}`
	req := httptest.NewRequest(http.MethodPost, "/sessions/no-such/mocks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
