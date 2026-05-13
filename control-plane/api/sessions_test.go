package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/session"
)

func newTestServer() http.Handler {
	return api.NewServer(session.NewStore(), adapters.NewRegistry())
}

func mustCreateSession(t *testing.T, srv http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/sessions", strings.NewReader(""))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	return body["id"]
}

func TestCreateSession(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/sessions", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["id"] == "" {
		t.Fatal("expected id in response")
	}
}

func TestDeleteSession(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	srv := api.NewServer(store, adapters.NewRegistry())
	req := httptest.NewRequest(http.MethodDelete, "/sessions/"+sess.ID, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if store.Get(sess.ID) != nil {
		t.Fatal("expected session to be deleted")
	}
}

func TestDeleteSessionNotFound(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest(http.MethodDelete, "/sessions/no-such-id", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
