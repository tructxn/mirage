# Mirage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a network-level mock proxy that intercepts all outbound TCP from a Docker service (HTTP, Redis, Kafka, MySQL, RabbitMQ) and returns configured fake responses via a single REST control plane.

**Architecture:** iptables redirects outbound traffic from the service container to Envoy, which routes per protocol to backend mock servers (WireMock, redis:alpine, Redpanda, mysql:8, rabbitmq). A Go control plane API manages mock rules per session and pushes them to each backend via their native APIs.

**Tech Stack:** Go 1.22, chi router, go-redis v9, franz-go (Kafka), Envoy 1.30, WireMock 3, docker-compose v3.8

---

## File Map

```
control-plane/
├── main.go                    — wire everything, start server on :9000
├── go.mod
├── api/
│   ├── server.go              — chi router, routes, middleware
│   ├── sessions.go            — POST /sessions, DELETE /sessions/{id}
│   ├── mocks.go               — POST/GET/DELETE /sessions/{id}/mocks
│   ├── traffic.go             — GET /sessions/{id}/traffic
│   └── status.go              — GET /status
├── session/
│   ├── types.go               — Session, Rule, TrafficRecord structs
│   └── store.go               — in-memory store, sync.RWMutex
└── adapters/
    ├── adapter.go             — BackendAdapter interface + Registry
    ├── wiremock.go            — WireMock admin HTTP API
    ├── redis.go               — go-redis adapter
    ├── kafka.go               — franz-go Kafka adapter (Phase 2)
    ├── mysql.go               — MySQL adapter (Phase 2)
    └── rabbitmq.go            — RabbitMQ management API adapter (Phase 3)

envoy/
├── envoy.yaml                 — static Envoy config (all listeners)
└── iptables.sh                — port redirect rules + UID exclusion

deploy/
└── docker-compose.yml         — full stack
```

---

## Phase 1 — HTTP + Redis (core loop)

### Task 1: Go module scaffold

**Files:**
- Create: `control-plane/go.mod`
- Create: `control-plane/main.go`

- [ ] **Step 1: Initialize Go module**

```bash
cd control-plane
go mod init github.com/tructxn/mirage/control-plane
```

Expected: `go.mod` created with module path.

- [ ] **Step 2: Add dependencies**

```bash
go get github.com/go-chi/chi/v5@latest
go get github.com/google/uuid@latest
go get github.com/redis/go-redis/v9@latest
```

- [ ] **Step 3: Write minimal main.go**

```go
// control-plane/main.go
package main

import (
	"log"
	"net/http"

	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func main() {
	store := session.NewStore()

	registry := adapters.NewRegistry()
	registry.Register("http", adapters.NewWireMockAdapter("http://wiremock:8080"))
	registry.Register("redis", adapters.NewRedisAdapter("redis:6380"))

	srv := api.NewServer(store, registry)

	log.Println("mirage control plane listening on :9000")
	if err := http.ListenAndServe(":9000", srv); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 4: Create stub directories**

```bash
mkdir -p api session adapters
```

- [ ] **Step 5: Commit**

```bash
git add control-plane/
git commit -m "feat: scaffold control-plane Go module"
```

---

### Task 2: Core types

**Files:**
- Create: `control-plane/session/types.go`

- [ ] **Step 1: Write types**

```go
// control-plane/session/types.go
package session

import "time"

type Rule struct {
	ID       string                 `json:"id"`
	Protocol string                 `json:"protocol"`
	Match    map[string]interface{} `json:"match"`
	Response interface{}            `json:"response"`
	Priority int                    `json:"priority"`
	// BackendID is the ID returned by the backend (e.g. WireMock stub UUID).
	BackendID string `json:"-"`
}

type TrafficRecord struct {
	Timestamp     time.Time              `json:"timestamp"`
	Protocol      string                 `json:"protocol"`
	Request       map[string]interface{} `json:"request"`
	MatchedRuleID *string                `json:"matched_rule_id"`
	NearMiss      *NearMiss              `json:"near_miss,omitempty"`
	Response      interface{}            `json:"response"`
	DurationMS    int64                  `json:"duration_ms"`
}

type NearMiss struct {
	RuleID     string `json:"rule_id"`
	FailedField string `json:"failed_field"`
}

type Session struct {
	ID    string
	Rules []Rule
}
```

- [ ] **Step 2: Commit**

```bash
git add control-plane/session/types.go
git commit -m "feat: add core session/rule/traffic types"
```

---

### Task 3: In-memory session store

**Files:**
- Create: `control-plane/session/store.go`
- Create: `control-plane/session/store_test.go`

- [ ] **Step 1: Write failing tests**

```go
// control-plane/session/store_test.go
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd control-plane && go test ./session/...
```

Expected: compile error — `session.NewStore` not defined.

- [ ] **Step 3: Implement the store**

```go
// control-plane/session/store.go
package session

import (
	"sync"

	"github.com/google/uuid"
)

type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

func (s *Store) Create() *Session {
	sess := &Session{ID: uuid.NewString()}
	s.mu.Lock()
	s.sessions[sess.ID] = sess
	s.mu.Unlock()
	return sess
}

func (s *Store) Get(id string) *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

func (s *Store) AddRule(sessionID string, rule Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		sess.Rules = append(sess.Rules, rule)
	}
}

func (s *Store) DeleteRule(sessionID, ruleID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	filtered := sess.Rules[:0]
	for _, r := range sess.Rules {
		if r.ID != ruleID {
			filtered = append(filtered, r)
		}
	}
	sess.Rules = filtered
}

func (s *Store) Rules(sessionID string) []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil
	}
	out := make([]Rule, len(sess.Rules))
	copy(out, sess.Rules)
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd control-plane && go test ./session/... -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add control-plane/session/
git commit -m "feat: in-memory session store with rule CRUD"
```

---

### Task 4: BackendAdapter interface and Registry

**Files:**
- Create: `control-plane/adapters/adapter.go`

- [ ] **Step 1: Write the interface and registry**

```go
// control-plane/adapters/adapter.go
package adapters

import "github.com/tructxn/mirage/control-plane/session"

// BackendAdapter is implemented by each protocol backend.
type BackendAdapter interface {
	// PushRule configures a mock rule on the backend.
	PushRule(sessionID string, rule *session.Rule) error
	// DeleteRule removes a mock rule from the backend.
	DeleteRule(sessionID string, rule session.Rule) error
	// Reset removes all rules for the session from the backend.
	Reset(sessionID string, rules []session.Rule) error
	// Traffic returns intercepted calls recorded by the backend for this session.
	Traffic(sessionID string) ([]session.TrafficRecord, error)
	// Healthy returns true if the backend is reachable.
	Healthy() bool
}

// Registry maps protocol names to their adapters.
type Registry struct {
	adapters map[string]BackendAdapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]BackendAdapter)}
}

func (r *Registry) Register(protocol string, adapter BackendAdapter) {
	r.adapters[protocol] = adapter
}

func (r *Registry) Get(protocol string) (BackendAdapter, bool) {
	a, ok := r.adapters[protocol]
	return a, ok
}

func (r *Registry) All() map[string]BackendAdapter {
	out := make(map[string]BackendAdapter, len(r.adapters))
	for k, v := range r.adapters {
		out[k] = v
	}
	return out
}
```

- [ ] **Step 2: Commit**

```bash
git add control-plane/adapters/adapter.go
git commit -m "feat: BackendAdapter interface and Registry"
```

---

### Task 5: API server setup

**Files:**
- Create: `control-plane/api/server.go`

- [ ] **Step 1: Write server**

```go
// control-plane/api/server.go
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

type Server struct {
	store    *session.Store
	registry *adapters.Registry
}

func NewServer(store *session.Store, registry *adapters.Registry) http.Handler {
	s := &Server{store: store, registry: registry}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/sessions", s.createSession)
	r.Delete("/sessions/{id}", s.deleteSession)

	r.Post("/sessions/{id}/mocks", s.createMock)
	r.Get("/sessions/{id}/mocks", s.listMocks)
	r.Delete("/sessions/{id}/mocks/{ruleId}", s.deleteMock)

	r.Get("/sessions/{id}/traffic", s.getTraffic)

	r.Get("/status", s.getStatus)

	return r
}
```

- [ ] **Step 2: Verify it compiles (handlers are stubs for now)**

Create stub files so it compiles:

```go
// control-plane/api/sessions.go
package api

import "net/http"

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {}
func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {}
```

```go
// control-plane/api/mocks.go
package api

import "net/http"

func (s *Server) createMock(w http.ResponseWriter, r *http.Request)  {}
func (s *Server) listMocks(w http.ResponseWriter, r *http.Request)   {}
func (s *Server) deleteMock(w http.ResponseWriter, r *http.Request)  {}
```

```go
// control-plane/api/traffic.go
package api

import "net/http"

func (s *Server) getTraffic(w http.ResponseWriter, r *http.Request) {}
```

```go
// control-plane/api/status.go
package api

import "net/http"

func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {}
```

```bash
cd control-plane && go build ./...
```

Expected: compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add control-plane/api/
git commit -m "feat: chi API server skeleton with all routes"
```

---

### Task 6: Sessions handlers

**Files:**
- Modify: `control-plane/api/sessions.go`
- Create: `control-plane/api/sessions_test.go`

- [ ] **Step 1: Write failing tests**

```go
// control-plane/api/sessions_test.go
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

// helper used by other test files
func mustCreateSession(t *testing.T, srv http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/sessions", strings.NewReader(""))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	return body["id"]
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd control-plane && go test ./api/... -run TestCreateSession -v
```

Expected: FAIL — handler returns 200 (stub).

- [ ] **Step 3: Implement sessions handlers**

```go
// control-plane/api/sessions.go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	sess := s.store.Create()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": sess.ID})
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	// Reset all rules in each backend for this session.
	rules := s.store.Rules(id)
	for proto, adapter := range s.registry.All() {
		protoRules := filterByProtocol(rules, proto)
		if len(protoRules) > 0 {
			_ = adapter.Reset(id, protoRules) // best-effort
		}
	}
	s.store.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

func filterByProtocol(rules []session.Rule, proto string) []session.Rule {
	var out []session.Rule
	for _, r := range rules {
		if r.Protocol == proto {
			out = append(out, r)
		}
	}
	return out
}
```

Add the missing import to sessions.go:
```go
import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tructxn/mirage/control-plane/session"
)
```

- [ ] **Step 4: Run tests**

```bash
cd control-plane && go test ./api/... -v
```

Expected: all session tests PASS.

- [ ] **Step 5: Commit**

```bash
git add control-plane/api/sessions.go control-plane/api/sessions_test.go
git commit -m "feat: sessions API handlers (create, delete)"
```

---

### Task 7: Mock rules handlers

**Files:**
- Modify: `control-plane/api/mocks.go`
- Create: `control-plane/api/mocks_test.go`

- [ ] **Step 1: Write failing tests**

```go
// control-plane/api/mocks_test.go
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd control-plane && go test ./api/... -run TestCreateMock -v
```

Expected: FAIL — stub handlers return nothing.

- [ ] **Step 3: Implement mocks handlers**

```go
// control-plane/api/mocks.go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tructxn/mirage/control-plane/session"
)

func (s *Server) createMock(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	var rule session.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	rule.ID = uuid.NewString()

	// Push to backend adapter if one is registered for this protocol.
	if adapter, ok := s.registry.Get(rule.Protocol); ok {
		if err := adapter.PushRule(id, &rule); err != nil {
			http.Error(w, "backend error: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	s.store.AddRule(id, rule)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": rule.ID})
}

func (s *Server) listMocks(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	rules := s.store.Rules(id)
	if rules == nil {
		rules = []session.Rule{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rules)
}

func (s *Server) deleteMock(w http.ResponseWriter, r *http.Request) {
	sessID := chi.URLParam(r, "id")
	ruleID := chi.URLParam(r, "ruleId")

	if s.store.Get(sessID) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	rules := s.store.Rules(sessID)
	var found *session.Rule
	for i := range rules {
		if rules[i].ID == ruleID {
			found = &rules[i]
			break
		}
	}
	if found == nil {
		http.Error(w, "rule not found", http.StatusNotFound)
		return
	}

	if adapter, ok := s.registry.Get(found.Protocol); ok {
		_ = adapter.DeleteRule(sessID, *found) // best-effort
	}
	s.store.DeleteRule(sessID, ruleID)
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 4: Run tests**

```bash
cd control-plane && go test ./api/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add control-plane/api/mocks.go control-plane/api/mocks_test.go
git commit -m "feat: mock rules API handlers (create, list, delete)"
```

---

### Task 8: Status handler

**Files:**
- Modify: `control-plane/api/status.go`

- [ ] **Step 1: Implement status handler**

```go
// control-plane/api/status.go
package api

import (
	"encoding/json"
	"net/http"
	"time"
)

type backendStatus struct {
	Healthy   bool      `json:"healthy"`
	CheckedAt time.Time `json:"checked_at"`
}

func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {
	result := make(map[string]backendStatus)
	for proto, adapter := range s.registry.All() {
		result[proto] = backendStatus{
			Healthy:   adapter.Healthy(),
			CheckedAt: time.Now().UTC(),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd control-plane && go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add control-plane/api/status.go
git commit -m "feat: GET /status endpoint with per-backend health"
```

---

### Task 9: Traffic handler (stub — Phase 4 will flesh out)

**Files:**
- Modify: `control-plane/api/traffic.go`

- [ ] **Step 1: Implement stub traffic handler**

```go
// control-plane/api/traffic.go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tructxn/mirage/control-plane/session"
)

func (s *Server) getTraffic(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	var records []session.TrafficRecord
	rules := s.store.Rules(id)

	// Pull traffic from each adapter that has rules for this session.
	seen := map[string]bool{}
	for _, rule := range rules {
		if seen[rule.Protocol] {
			continue
		}
		seen[rule.Protocol] = true
		if adapter, ok := s.registry.Get(rule.Protocol); ok {
			got, err := adapter.Traffic(id)
			if err == nil {
				records = append(records, got...)
			}
		}
	}
	if records == nil {
		records = []session.TrafficRecord{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}
```

- [ ] **Step 2: Commit**

```bash
git add control-plane/api/traffic.go
git commit -m "feat: GET /sessions/{id}/traffic stub (pulls from adapters)"
```

---

### Task 10: WireMock adapter

**Files:**
- Create: `control-plane/adapters/wiremock.go`
- Create: `control-plane/adapters/wiremock_test.go`

- [ ] **Step 1: Write failing unit test (with mock HTTP server)**

```go
// control-plane/adapters/wiremock_test.go
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
	// Fake WireMock that records the incoming stub request.
	var received map[string]interface{}
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
		Match:    map[string]interface{}{"method": "GET", "path": "/ping"},
		Response: map[string]interface{}{"status": 200, "body": "pong"},
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
	adapter := adapters.NewWireMockAdapter(ts.URL)
	if !adapter.Healthy() {
		t.Fatal("expected Healthy()=true")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd control-plane && go test ./adapters/... -run TestWireMock -v
```

Expected: compile error — `adapters.NewWireMockAdapter` not defined.

- [ ] **Step 3: Implement WireMock adapter**

```go
// control-plane/adapters/wiremock.go
package adapters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tructxn/mirage/control-plane/session"
)

type WireMockAdapter struct {
	baseURL string
	client  *http.Client
}

func NewWireMockAdapter(baseURL string) *WireMockAdapter {
	return &WireMockAdapter{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

// PushRule creates a WireMock stub from the rule.
// Stores the WireMock stub UUID in rule.BackendID.
func (a *WireMockAdapter) PushRule(sessionID string, rule *session.Rule) error {
	match, _ := rule.Match["method"].(string)
	path, _ := rule.Match["path"].(string)

	resp, _ := rule.Response.(map[string]interface{})
	status := 200
	if s, ok := resp["status"].(float64); ok {
		status = int(s)
	}
	body := ""
	if b, ok := resp["body"].(string); ok {
		body = b
	}

	payload := map[string]interface{}{
		"request": map[string]interface{}{
			"method": match,
			"url":    path,
		},
		"response": map[string]interface{}{
			"status": status,
			"body":   body,
		},
		"metadata": map[string]string{
			"mirage-session": sessionID,
			"mirage-rule":    rule.ID,
		},
	}

	data, _ := json.Marshal(payload)
	res, err := a.client.Post(a.baseURL+"/__admin/mappings", "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("wiremock post: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("wiremock returned %d", res.StatusCode)
	}

	var wmResp map[string]string
	json.NewDecoder(res.Body).Decode(&wmResp)
	rule.BackendID = wmResp["id"]
	return nil
}

func (a *WireMockAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	if rule.BackendID == "" {
		return nil
	}
	req, _ := http.NewRequest(http.MethodDelete, a.baseURL+"/__admin/mappings/"+rule.BackendID, nil)
	res, err := a.client.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

func (a *WireMockAdapter) Reset(sessionID string, rules []session.Rule) error {
	for _, r := range rules {
		_ = a.DeleteRule(sessionID, r)
	}
	return nil
}

func (a *WireMockAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	// WireMock records all requests at /__admin/requests
	res, err := a.client.Get(a.baseURL + "/__admin/requests")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var wmRequests struct {
		Requests []struct {
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			WasMatched bool   `json:"wasMatched"`
			StubID     string `json:"stubMappingId"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(res.Body).Decode(&wmRequests); err != nil {
		return nil, err
	}

	var records []session.TrafficRecord
	for _, req := range wmRequests.Requests {
		rec := session.TrafficRecord{
			Timestamp: time.Now().UTC(),
			Protocol:  "http",
			Request: map[string]interface{}{
				"method": req.Request.Method,
				"url":    req.Request.URL,
			},
		}
		if req.WasMatched && req.StubID != "" {
			// Find the mirage rule ID that maps to this stub.
			// Simplified: store stub ID as matched rule ID for now.
			id := req.StubID
			rec.MatchedRuleID = &id
		}
		records = append(records, rec)
	}
	return records, nil
}

func (a *WireMockAdapter) Healthy() bool {
	res, err := a.client.Get(a.baseURL + "/__admin/health")
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode == http.StatusOK
}
```

- [ ] **Step 4: Run tests**

```bash
cd control-plane && go test ./adapters/... -run TestWireMock -v
```

Expected: all WireMock tests PASS.

- [ ] **Step 5: Commit**

```bash
git add control-plane/adapters/wiremock.go control-plane/adapters/wiremock_test.go
git commit -m "feat: WireMock adapter (push, delete, reset, traffic, health)"
```

---

### Task 11: Redis adapter

**Files:**
- Create: `control-plane/adapters/redis.go`
- Create: `control-plane/adapters/redis_test.go`

- [ ] **Step 1: Add test dependency (miniredis for tests)**

```bash
cd control-plane && go get github.com/alicebob/miniredis/v2@latest
```

- [ ] **Step 2: Write failing tests**

```go
// control-plane/adapters/redis_test.go
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
		Match:    map[string]interface{}{"command": "GET", "key": "user:42"},
		Response: `{"id":42}`,
	}

	if err := adapter.PushRule("sess-1", rule); err != nil {
		t.Fatalf("PushRule failed: %v", err)
	}

	// Verify key was SET in Redis.
	val := mr.HGet("mirage:sess-1", "rule-1:user:42")
	if val == "" {
		val, _ = mr.Get("user:42")
	}
	// The adapter should have SET user:42 → {"id":42}
	got, err := mr.Get("user:42")
	if err != nil {
		t.Fatalf("key user:42 not found in Redis: %v", err)
	}
	if got != `{"id":42}` {
		t.Fatalf("expected {\"id\":42}, got %q", got)
	}
}

func TestRedisReset(t *testing.T) {
	mr := miniredis.RunT(t)
	adapter := adapters.NewRedisAdapter(mr.Addr())

	rule := &session.Rule{
		ID:       "r1",
		Protocol: "redis",
		Match:    map[string]interface{}{"command": "GET", "key": "some-key"},
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
	adapter := adapters.NewRedisAdapter(mr.Addr())
	if !adapter.Healthy() {
		t.Fatal("expected Healthy()=true")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd control-plane && go test ./adapters/... -run TestRedis -v
```

Expected: compile error — `adapters.NewRedisAdapter` not defined.

- [ ] **Step 4: Implement Redis adapter**

```go
// control-plane/adapters/redis.go
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
	if !ok {
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

// Traffic is not supported by redis:alpine natively.
// Returns empty slice — Phase 4 will add proper traffic recording.
func (a *RedisAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil
}

func (a *RedisAdapter) Healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return a.rdb.Ping(ctx).Err() == nil
}
```

- [ ] **Step 5: Run tests**

```bash
cd control-plane && go test ./adapters/... -run TestRedis -v
```

Expected: all Redis tests PASS.

- [ ] **Step 6: Commit**

```bash
git add control-plane/adapters/redis.go control-plane/adapters/redis_test.go
git commit -m "feat: Redis adapter (exact key SET/DEL, health check)"
```

---

### Task 12: Envoy static config (HTTP + Redis)

**Files:**
- Create: `envoy/envoy.yaml`

- [ ] **Step 1: Write Envoy config**

```yaml
# envoy/envoy.yaml
static_resources:
  listeners:
    # HTTP (port 80)
    - name: listener_http
      address:
        socket_address: { address: 0.0.0.0, port_value: 15001 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress_http
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: backend
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route: { cluster: wiremock }
                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

    # HTTPS (port 443) — same upstream as HTTP for now
    - name: listener_https
      address:
        socket_address: { address: 0.0.0.0, port_value: 15002 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress_https
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: backend
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route: { cluster: wiremock }
                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

    # Redis (port 6379)
    - name: listener_redis
      address:
        socket_address: { address: 0.0.0.0, port_value: 15003 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.redis_proxy
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
                stat_prefix: egress_redis
                settings:
                  op_timeout: 5s
                prefix_routes:
                  catch_all_route:
                    cluster: redis_mock

  clusters:
    - name: wiremock
      connect_timeout: 5s
      type: STRICT_DNS
      load_assignment:
        cluster_name: wiremock
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: wiremock, port_value: 8080 }

    - name: redis_mock
      connect_timeout: 5s
      type: STRICT_DNS
      load_assignment:
        cluster_name: redis_mock
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: redis, port_value: 6380 }

admin:
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }
```

- [ ] **Step 2: Commit**

```bash
git add envoy/envoy.yaml
git commit -m "feat: Envoy static config for HTTP and Redis listeners"
```

---

### Task 13: iptables script

**Files:**
- Create: `envoy/iptables.sh`

- [ ] **Step 1: Write iptables script**

```bash
#!/bin/sh
# envoy/iptables.sh
# Run as root before Envoy starts.
# Sets up outbound traffic redirection for the shared network namespace.
set -e

# Envoy runs as UID 1337. Exclude its own traffic to prevent redirect loops.
iptables -t nat -N MIRAGE_REDIRECT 2>/dev/null || true
iptables -t nat -F MIRAGE_REDIRECT

# Skip Envoy's own outbound traffic.
iptables -t nat -A OUTPUT -m owner --uid-owner 1337 -j RETURN

# Redirect per protocol port to Envoy listeners.
iptables -t nat -A OUTPUT -p tcp --dport 80   -j REDIRECT --to-port 15001
iptables -t nat -A OUTPUT -p tcp --dport 443  -j REDIRECT --to-port 15002
iptables -t nat -A OUTPUT -p tcp --dport 6379 -j REDIRECT --to-port 15003
# Ports below are configured in Phase 2 and 3.
# iptables -t nat -A OUTPUT -p tcp --dport 9092 -j REDIRECT --to-port 15004
# iptables -t nat -A OUTPUT -p tcp --dport 3306 -j REDIRECT --to-port 15005
# iptables -t nat -A OUTPUT -p tcp --dport 5672 -j REDIRECT --to-port 15006

echo "iptables rules applied"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x envoy/iptables.sh
```

- [ ] **Step 3: Commit**

```bash
git add envoy/iptables.sh
git commit -m "feat: iptables redirect script with UID exclusion for Envoy"
```

---

### Task 14: docker-compose Phase 1

**Files:**
- Create: `deploy/docker-compose.yml`
- Create: `deploy/Dockerfile.control-plane`
- Create: `deploy/Dockerfile.envoy`

- [ ] **Step 1: Write control-plane Dockerfile**

```dockerfile
# deploy/Dockerfile.control-plane
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY control-plane/ .
RUN go build -o mirage-cp .

FROM alpine:3.19
COPY --from=builder /app/mirage-cp /mirage-cp
EXPOSE 9000
ENTRYPOINT ["/mirage-cp"]
```

- [ ] **Step 2: Write Envoy Dockerfile**

```dockerfile
# deploy/Dockerfile.envoy
FROM envoyproxy/envoy:v1.30-latest
USER root
RUN apt-get update && apt-get install -y iptables && rm -rf /var/lib/apt/lists/*
COPY envoy/iptables.sh /iptables.sh
COPY envoy/envoy.yaml /etc/envoy/envoy.yaml
RUN chmod +x /iptables.sh
# Run as UID 1337 so iptables exclusion rule works.
RUN useradd -u 1337 envoy || true
ENTRYPOINT ["/bin/sh", "-c", "/iptables.sh && su envoy -s /bin/sh -c 'envoy -c /etc/envoy/envoy.yaml'"]
```

- [ ] **Step 3: Write docker-compose.yml**

```yaml
# deploy/docker-compose.yml
version: "3.8"

services:
  mirage-envoy:
    build:
      context: ..
      dockerfile: deploy/Dockerfile.envoy
    cap_add:
      - NET_ADMIN
    ports:
      - "9901:9901"   # Envoy admin UI

  mirage-cp:
    build:
      context: ..
      dockerfile: deploy/Dockerfile.control-plane
    ports:
      - "9000:9000"
    environment:
      WIREMOCK_URL: http://wiremock:8080
      REDIS_ADDR: redis:6380

  wiremock:
    image: wiremock/wiremock:3.5.4
    command: ["--port", "8080", "--record-mappings"]
    ports:
      - "8080:8080"

  redis:
    image: redis:7-alpine
    command: redis-server --port 6380
    ports:
      - "6380:6380"

  # your-service shares Envoy's network namespace so iptables rules apply.
  # Uncomment and fill in your service image when testing.
  # your-service:
  #   image: your-service:latest
  #   network_mode: service:mirage-envoy
  #   depends_on:
  #     - mirage-envoy
  #     - mirage-cp
```

- [ ] **Step 4: Build and start Phase 1 stack**

```bash
cd deploy && docker compose up --build -d mirage-envoy mirage-cp wiremock redis
```

Expected: all 4 containers start without errors.

- [ ] **Step 5: Smoke test the control plane**

```bash
# Create a session
SESSION=$(curl -s -X POST http://localhost:9000/sessions | jq -r .id)
echo "Session: $SESSION"

# Add an HTTP mock
curl -s -X POST http://localhost:9000/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{"protocol":"http","match":{"method":"GET","path":"/ping"},"response":{"status":200,"body":"pong"}}'

# Verify WireMock stub exists
curl -s http://localhost:8080/ping

# Check status
curl -s http://localhost:9000/status
```

Expected: `/ping` returns `pong`. Status shows `http.healthy: true` and `redis.healthy: true`.

- [ ] **Step 6: Commit**

```bash
cd ..
git add deploy/
git commit -m "feat: docker-compose Phase 1 with Envoy, WireMock, Redis"
```

---

### Task 15: Add environment-based config to control plane

**Files:**
- Modify: `control-plane/main.go`

Right now `main.go` has hardcoded addresses. Replace with env vars so docker-compose can configure them.

- [ ] **Step 1: Update main.go**

```go
// control-plane/main.go
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/session"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	store := session.NewStore()
	registry := adapters.NewRegistry()

	wireMockURL := getenv("WIREMOCK_URL", "http://wiremock:8080")
	registry.Register("http", adapters.NewWireMockAdapter(wireMockURL))

	redisAddr := getenv("REDIS_ADDR", "redis:6380")
	registry.Register("redis", adapters.NewRedisAdapter(redisAddr))

	srv := api.NewServer(store, registry)

	addr := getenv("LISTEN_ADDR", ":9000")
	log.Printf("mirage control plane listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Build to verify**

```bash
cd control-plane && go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add control-plane/main.go
git commit -m "feat: environment-based config for control plane addresses"
```

---

## Phase 2 — Kafka + MySQL

### Task 16: Envoy config — add Kafka + MySQL listeners

**Files:**
- Modify: `envoy/envoy.yaml`
- Modify: `envoy/iptables.sh`

- [ ] **Step 1: Append Kafka and MySQL listeners to envoy.yaml**

Add to `static_resources.listeners`:

```yaml
    # Kafka (port 9092)
    - name: listener_kafka
      address:
        socket_address: { address: 0.0.0.0, port_value: 15004 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.kafka_broker
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.kafka_broker.v3.KafkaBroker
                stat_prefix: egress_kafka
            - name: envoy.filters.network.tcp_proxy
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
                stat_prefix: egress_kafka_tcp
                cluster: kafka_mock

    # MySQL (port 3306)
    - name: listener_mysql
      address:
        socket_address: { address: 0.0.0.0, port_value: 15005 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.mysql_proxy
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.mysql_proxy.v3.MySQLProxy
                stat_prefix: egress_mysql
            - name: envoy.filters.network.tcp_proxy
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
                stat_prefix: egress_mysql_tcp
                cluster: mysql_mock
```

Add to `static_resources.clusters`:

```yaml
    - name: kafka_mock
      connect_timeout: 5s
      type: STRICT_DNS
      load_assignment:
        cluster_name: kafka_mock
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: redpanda, port_value: 9093 }

    - name: mysql_mock
      connect_timeout: 5s
      type: STRICT_DNS
      load_assignment:
        cluster_name: mysql_mock
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: mysql, port_value: 3307 }
```

- [ ] **Step 2: Uncomment Kafka + MySQL lines in iptables.sh**

```bash
# In envoy/iptables.sh, uncomment these two lines:
iptables -t nat -A OUTPUT -p tcp --dport 9092 -j REDIRECT --to-port 15004
iptables -t nat -A OUTPUT -p tcp --dport 3306 -j REDIRECT --to-port 15005
```

- [ ] **Step 3: Commit**

```bash
git add envoy/envoy.yaml envoy/iptables.sh
git commit -m "feat: Envoy listeners for Kafka and MySQL (Phase 2)"
```

---

### Task 17: Kafka adapter (Redpanda)

**Files:**
- Create: `control-plane/adapters/kafka.go`
- Create: `control-plane/adapters/kafka_test.go`

- [ ] **Step 1: Add franz-go dependency**

```bash
cd control-plane && go get github.com/twmb/franz-go/pkg/kgo@latest
go get github.com/twmb/franz-go/pkg/kadm@latest
```

- [ ] **Step 2: Write failing test**

```go
// control-plane/adapters/kafka_test.go
package adapters_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

// KafkaAdapter unit test: mocking at the produce-messages level.
// Integration tests (requiring real Redpanda) are in adapters/integration_test.go.
func TestKafkaAdapterHealthyUnreachable(t *testing.T) {
	adapter := adapters.NewKafkaAdapter("localhost:19999") // nothing listening
	if adapter.Healthy() {
		t.Fatal("expected Healthy()=false for unreachable broker")
	}
}

func TestKafkaPushRuleMissingTopic(t *testing.T) {
	adapter := adapters.NewKafkaAdapter("localhost:19999")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "kafka",
		Match:    map[string]interface{}{}, // no topic
		Response: "some-message",
	}
	err := adapter.PushRule("sess-1", rule)
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd control-plane && go test ./adapters/... -run TestKafka -v
```

Expected: compile error.

- [ ] **Step 4: Implement Kafka adapter**

```go
// control-plane/adapters/kafka.go
package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/tructxn/mirage/control-plane/session"
)

type KafkaAdapter struct {
	brokers []string
}

func NewKafkaAdapter(broker string) *KafkaAdapter {
	return &KafkaAdapter{brokers: []string{broker}}
}

// PushRule produces a message to the configured topic so consumers receive it.
func (a *KafkaAdapter) PushRule(sessionID string, rule *session.Rule) error {
	topic, ok := rule.Match["topic"].(string)
	if !ok || topic == "" {
		return fmt.Errorf("kafka rule missing match.topic")
	}

	val := fmt.Sprintf("%v", rule.Response)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(a.brokers...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(val),
		Headers: []kgo.RecordHeader{
			{Key: "mirage-session", Value: []byte(sessionID)},
			{Key: "mirage-rule", Value: []byte(rule.ID)},
		},
	})
	return results.FirstErr()
}

func (a *KafkaAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	// Kafka messages are immutable — no delete. Rules are scoped by session header.
	return nil
}

func (a *KafkaAdapter) Reset(sessionID string, rules []session.Rule) error {
	// For reset, delete and recreate any topics used by this session's rules.
	topics := map[string]bool{}
	for _, r := range rules {
		if t, ok := r.Match["topic"].(string); ok {
			topics[t] = true
		}
	}
	if len(topics) == 0 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(a.brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	names := make([]string, 0, len(topics))
	for t := range topics {
		names = append(names, t)
	}
	_, _ = adm.DeleteTopics(ctx, names...)
	return nil
}

func (a *KafkaAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	// Traffic inspection via Redpanda admin API is Phase 4.
	return nil, nil
}

func (a *KafkaAdapter) Healthy() bool {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(a.brokers...),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return cl.Ping(ctx) == nil
}
```

- [ ] **Step 5: Run tests**

```bash
cd control-plane && go test ./adapters/... -run TestKafka -v
```

Expected: all Kafka tests PASS.

- [ ] **Step 6: Register Kafka adapter in main.go**

```go
// In control-plane/main.go, add after Redis registration:
kafkaBroker := getenv("KAFKA_BROKER", "redpanda:9093")
registry.Register("kafka", adapters.NewKafkaAdapter(kafkaBroker))
```

- [ ] **Step 7: Commit**

```bash
git add control-plane/adapters/kafka.go control-plane/adapters/kafka_test.go control-plane/main.go
git commit -m "feat: Kafka adapter with Redpanda (produce/reset/health)"
```

---

### Task 18: MySQL adapter

**Files:**
- Create: `control-plane/adapters/mysql.go`
- Create: `control-plane/adapters/mysql_test.go`

- [ ] **Step 1: Add go-sql-driver dependency**

```bash
cd control-plane && go get github.com/go-sql-driver/mysql@latest
```

- [ ] **Step 2: Write failing tests**

```go
// control-plane/adapters/mysql_test.go
package adapters_test

import (
	"testing"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

func TestMySQLPushRuleMissingQuery(t *testing.T) {
	adapter := adapters.NewMySQLAdapter("root:@tcp(localhost:13307)/mirage")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "mysql",
		Match:    map[string]interface{}{}, // no query
		Response: []map[string]interface{}{},
	}
	if err := adapter.PushRule("sess-1", rule); err == nil {
		t.Fatal("expected error for missing query")
	}
}

func TestMySQLHealthyUnreachable(t *testing.T) {
	adapter := adapters.NewMySQLAdapter("root:@tcp(localhost:19999)/mirage")
	if adapter.Healthy() {
		t.Fatal("expected Healthy()=false for unreachable MySQL")
	}
}
```

- [ ] **Step 3: Run to verify they fail**

```bash
cd control-plane && go test ./adapters/... -run TestMySQL -v
```

Expected: compile error.

- [ ] **Step 4: Implement MySQL adapter**

The MySQL adapter uses a real `mysql:8.0` container. Mock rules insert rows into a `mirage_mocks` table; the service's queries are answered by returning those rows. This requires the service to query through a view or stored procedure — simple pass-through for exact table data.

```go
// control-plane/adapters/mysql.go
package adapters

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tructxn/mirage/control-plane/session"
)

type MySQLAdapter struct {
	dsn string
	db  *sql.DB
}

func NewMySQLAdapter(dsn string) *MySQLAdapter {
	db, _ := sql.Open("mysql", dsn)
	db.SetConnMaxLifetime(time.Minute)
	return &MySQLAdapter{dsn: dsn, db: db}
}

// PushRule inserts a mock row into the target table.
// match.table and match.where identify the row; response is the JSON-encoded columns.
func (a *MySQLAdapter) PushRule(sessionID string, rule *session.Rule) error {
	table, ok := rule.Match["table"].(string)
	if !ok || table == "" {
		return fmt.Errorf("mysql rule missing match.table")
	}

	rows, ok := rule.Response.([]interface{})
	if !ok {
		return fmt.Errorf("mysql rule response must be an array of row objects")
	}

	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		_, err = a.db.Exec(
			`INSERT INTO mirage_shadow (session_id, rule_id, target_table, row_data) VALUES (?, ?, ?, ?)`,
			sessionID, rule.ID, table, string(data),
		)
		if err != nil {
			return fmt.Errorf("mysql insert shadow row: %w", err)
		}
	}
	return nil
}

func (a *MySQLAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	_, err := a.db.Exec(`DELETE FROM mirage_shadow WHERE session_id=? AND rule_id=?`, sessionID, rule.ID)
	return err
}

func (a *MySQLAdapter) Reset(sessionID string, rules []session.Rule) error {
	_, err := a.db.Exec(`DELETE FROM mirage_shadow WHERE session_id=?`, sessionID)
	return err
}

func (a *MySQLAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil // Phase 4
}

func (a *MySQLAdapter) Healthy() bool {
	if err := a.db.Ping(); err != nil {
		return false
	}
	return true
}
```

- [ ] **Step 5: Run tests**

```bash
cd control-plane && go test ./adapters/... -run TestMySQL -v
```

Expected: all MySQL tests PASS.

- [ ] **Step 6: Register MySQL adapter in main.go**

```go
// In control-plane/main.go, add:
mysqlDSN := getenv("MYSQL_DSN", "root:mirage@tcp(mysql:3307)/mirage")
registry.Register("mysql", adapters.NewMySQLAdapter(mysqlDSN))
```

- [ ] **Step 7: Add Redpanda + MySQL to docker-compose**

```yaml
# Add to deploy/docker-compose.yml services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9093,external://0.0.0.0:9092
      - --advertise-kafka-addr internal://redpanda:9093,external://localhost:9092
    ports:
      - "9092:9092"
      - "9093:9093"

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: mirage
      MYSQL_DATABASE: mirage
    command: --port=3307
    ports:
      - "3307:3307"
```

Also add to `mirage-cp` environment:
```yaml
      KAFKA_BROKER: redpanda:9093
      MYSQL_DSN: root:mirage@tcp(mysql:3307)/mirage
```

- [ ] **Step 8: Commit**

```bash
git add control-plane/adapters/mysql.go control-plane/adapters/mysql_test.go
git add control-plane/main.go deploy/docker-compose.yml
git commit -m "feat: MySQL adapter and Redpanda/MySQL in docker-compose (Phase 2)"
```

---

## Phase 3 — RabbitMQ

### Task 19: RabbitMQ adapter

**Files:**
- Create: `control-plane/adapters/rabbitmq.go`
- Create: `control-plane/adapters/rabbitmq_test.go`

- [ ] **Step 1: Add amqp091-go dependency**

```bash
cd control-plane && go get github.com/rabbitmq/amqp091-go@latest
```

- [ ] **Step 2: Write failing tests**

```go
// control-plane/adapters/rabbitmq_test.go
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
		Match:    map[string]interface{}{}, // no exchange
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
```

- [ ] **Step 3: Run to verify they fail**

```bash
cd control-plane && go test ./adapters/... -run TestRabbitMQ -v
```

Expected: compile error.

- [ ] **Step 4: Implement RabbitMQ adapter**

```go
// control-plane/adapters/rabbitmq.go
package adapters

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

type RabbitMQAdapter struct {
	url string
}

func NewRabbitMQAdapter(url string) *RabbitMQAdapter {
	return &RabbitMQAdapter{url: url}
}

func (a *RabbitMQAdapter) connect() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

// PushRule publishes a message to the configured exchange + routing key.
// The service consuming from that queue will receive this mock message.
func (a *RabbitMQAdapter) PushRule(sessionID string, rule *session.Rule) error {
	exchange, ok := rule.Match["exchange"].(string)
	if !ok || exchange == "" {
		return fmt.Errorf("amqp rule missing match.exchange")
	}
	routingKey, _ := rule.Match["routing_key"].(string)
	body := fmt.Sprintf("%v", rule.Response)

	conn, ch, err := a.connect()
	if err != nil {
		return fmt.Errorf("rabbitmq connect: %w", err)
	}
	defer conn.Close()
	defer ch.Close()

	return ch.Publish(exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         []byte(body),
		DeliveryMode: amqp.Persistent,
		Headers: amqp.Table{
			"mirage-session": sessionID,
			"mirage-rule":    rule.ID,
		},
	})
}

func (a *RabbitMQAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	// AMQP messages are consumed — no delete concept. Noop.
	return nil
}

func (a *RabbitMQAdapter) Reset(sessionID string, rules []session.Rule) error {
	// Purge queues used by this session's rules.
	queues := map[string]bool{}
	for _, r := range rules {
		if q, ok := r.Match["queue"].(string); ok {
			queues[q] = true
		}
	}
	if len(queues) == 0 {
		return nil
	}
	conn, ch, err := a.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()
	for q := range queues {
		_, _ = ch.QueuePurge(q, false)
	}
	return nil
}

func (a *RabbitMQAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil // Phase 4
}

func (a *RabbitMQAdapter) Healthy() bool {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
```

- [ ] **Step 5: Run tests**

```bash
cd control-plane && go test ./adapters/... -run TestRabbitMQ -v
```

Expected: all RabbitMQ tests PASS.

- [ ] **Step 6: Register RabbitMQ adapter in main.go**

```go
// In control-plane/main.go, add:
rabbitURL := getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5673/")
registry.Register("amqp", adapters.NewRabbitMQAdapter(rabbitURL))
```

- [ ] **Step 7: Add RabbitMQ to docker-compose and Envoy**

In `deploy/docker-compose.yml`, add:
```yaml
  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    ports:
      - "5673:5673"
      - "15673:15672"
    environment:
      RABBITMQ_NODE_PORT: 5673
```

Add to `mirage-cp` environment:
```yaml
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5673/
```

In `envoy/envoy.yaml`, add listener for AMQP:
```yaml
    - name: listener_amqp
      address:
        socket_address: { address: 0.0.0.0, port_value: 15006 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.tcp_proxy
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
                stat_prefix: egress_amqp
                cluster: rabbitmq_mock
```

Add cluster:
```yaml
    - name: rabbitmq_mock
      connect_timeout: 5s
      type: STRICT_DNS
      load_assignment:
        cluster_name: rabbitmq_mock
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: rabbitmq, port_value: 5673 }
```

Uncomment in `envoy/iptables.sh`:
```bash
iptables -t nat -A OUTPUT -p tcp --dport 5672 -j REDIRECT --to-port 15006
```

- [ ] **Step 8: Commit**

```bash
git add control-plane/adapters/rabbitmq.go control-plane/adapters/rabbitmq_test.go
git add control-plane/main.go deploy/docker-compose.yml envoy/envoy.yaml envoy/iptables.sh
git commit -m "feat: RabbitMQ adapter, Phase 3 wiring (AMQP listener, docker-compose)"
```

---

## Phase 4 — Traffic + Diagnostics

### Task 20: WireMock traffic with rule mapping

**Files:**
- Modify: `control-plane/adapters/wiremock.go`
- Modify: `control-plane/session/store.go`

The WireMock adapter already fetches `/__admin/requests`. We need to map WireMock stub IDs back to Mirage rule IDs. The store needs a lookup by backend ID.

- [ ] **Step 1: Add BackendID index to store**

```go
// In control-plane/session/store.go, add method:
func (s *Store) RuleByBackendID(sessionID, backendID string) *Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil
	}
	for i := range sess.Rules {
		if sess.Rules[i].BackendID == backendID {
			return &sess.Rules[i]
		}
	}
	return nil
}
```

- [ ] **Step 2: Write test**

```go
// In control-plane/session/store_test.go, add:
func TestRuleByBackendID(t *testing.T) {
	store := session.NewStore()
	sess := store.Create()
	store.AddRule(sess.ID, session.Rule{ID: "r1", BackendID: "wm-123", Protocol: "http"})
	rule := store.RuleByBackendID(sess.ID, "wm-123")
	if rule == nil || rule.ID != "r1" {
		t.Fatal("expected to find rule by backend ID")
	}
}
```

- [ ] **Step 3: Run test**

```bash
cd control-plane && go test ./session/... -v
```

Expected: all PASS.

- [ ] **Step 4: Update WireMock Traffic() to use Mirage rule IDs**

```go
// In control-plane/adapters/wiremock.go, update Traffic():
// Add store parameter to WireMockAdapter struct:
type WireMockAdapter struct {
	baseURL string
	client  *http.Client
	store   RuleStore // interface below
}

// RuleStore is the subset of session.Store used by adapters.
type RuleStore interface {
	RuleByBackendID(sessionID, backendID string) *session.Rule
}
```

Update `NewWireMockAdapter`:
```go
func NewWireMockAdapter(baseURL string) *WireMockAdapter {
	return &WireMockAdapter{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *WireMockAdapter) WithStore(store RuleStore) *WireMockAdapter {
	a.store = store
	return a
}
```

Update `Traffic()` to map stub ID → Mirage rule ID:
```go
func (a *WireMockAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	res, err := a.client.Get(a.baseURL + "/__admin/requests")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var wmRequests struct {
		Requests []struct {
			Request struct {
				Method  string            `json:"method"`
				URL     string            `json:"url"`
				Headers map[string]string `json:"headers"`
			} `json:"request"`
			WasMatched    bool   `json:"wasMatched"`
			StubMappingID string `json:"stubMappingId"`
			ResponseDef   struct {
				Status int    `json:"status"`
				Body   string `json:"body"`
			} `json:"responseDefinition"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(res.Body).Decode(&wmRequests); err != nil {
		return nil, err
	}

	var records []session.TrafficRecord
	for _, req := range wmRequests.Requests {
		rec := session.TrafficRecord{
			Timestamp: time.Now().UTC(),
			Protocol:  "http",
			Request: map[string]interface{}{
				"method":  req.Request.Method,
				"url":     req.Request.URL,
				"headers": req.Request.Headers,
			},
			Response: map[string]interface{}{
				"status": req.ResponseDef.Status,
				"body":   req.ResponseDef.Body,
			},
		}
		if req.WasMatched && req.StubMappingID != "" && a.store != nil {
			if rule := a.store.RuleByBackendID(sessionID, req.StubMappingID); rule != nil {
				rec.MatchedRuleID = &rule.ID
			}
		}
		records = append(records, rec)
	}
	return records, nil
}
```

- [ ] **Step 5: Update main.go to wire store into WireMock adapter**

```go
// In main.go:
wmAdapter := adapters.NewWireMockAdapter(wireMockURL).WithStore(store)
registry.Register("http", wmAdapter)
```

- [ ] **Step 6: Run all tests**

```bash
cd control-plane && go test ./... -v
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add control-plane/
git commit -m "feat: WireMock traffic with Mirage rule ID mapping"
```

---

### Task 21: Near-miss diagnostics for HTTP

**Files:**
- Modify: `control-plane/adapters/wiremock.go`

WireMock's `/__admin/requests/unmatched` returns near-miss info natively.

- [ ] **Step 1: Add near-miss fetch to Traffic()**

```go
// In wiremock.go Traffic(), after fetching matched requests, also fetch unmatched:
func (a *WireMockAdapter) fetchNearMisses() ([]session.TrafficRecord, error) {
	res, err := a.client.Get(a.baseURL + "/__admin/requests/unmatched")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var payload struct {
		Requests []struct {
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			NearMissRequests []struct {
				Mapping struct {
					ID string `json:"id"`
				} `json:"mapping"`
				RequestMatchResult struct {
					MatchFields []struct {
						Key     string `json:"key"`
						Matched bool   `json:"matched"`
					} `json:"attributeMatchResults"`
				} `json:"requestMatchResult"`
			} `json:"nearMissRequests"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}

	var records []session.TrafficRecord
	for _, req := range payload.Requests {
		rec := session.TrafficRecord{
			Timestamp: time.Now().UTC(),
			Protocol:  "http",
			Request: map[string]interface{}{
				"method": req.Request.Method,
				"url":    req.Request.URL,
			},
		}
		// Attach near-miss info from closest rule.
		if len(req.NearMissRequests) > 0 {
			nm := req.NearMissRequests[0]
			failedField := "unknown"
			for _, f := range nm.RequestMatchResult.MatchFields {
				if !f.Matched {
					failedField = f.Key
					break
				}
			}
			stubID := nm.Mapping.ID
			var ruleID string
			if a.store != nil {
				if rule := a.store.RuleByBackendID("", stubID); rule != nil {
					ruleID = rule.ID
				}
			}
			rec.NearMiss = &session.NearMiss{
				RuleID:      ruleID,
				FailedField: failedField,
			}
		}
		records = append(records, rec)
	}
	return records, nil
}
```

Update `Traffic()` to merge matched + unmatched:
```go
func (a *WireMockAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	matched, err := a.fetchMatchedRequests(sessionID)
	if err != nil {
		return nil, err
	}
	unmatched, _ := a.fetchNearMisses() // best-effort, don't fail if unavailable
	return append(matched, unmatched...), nil
}
```

Rename the existing Traffic() body to `fetchMatchedRequests()`.

- [ ] **Step 2: Run all tests**

```bash
cd control-plane && go test ./... -v
```

Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add control-plane/adapters/wiremock.go
git commit -m "feat: near-miss diagnostics for HTTP via WireMock unmatched requests"
```

---

### Task 22: Final integration smoke test + run all tests

- [ ] **Step 1: Start full stack**

```bash
cd deploy && docker compose up --build -d
```

Expected: all containers healthy.

- [ ] **Step 2: Run full E2E smoke test**

```bash
#!/bin/bash
set -e

BASE=http://localhost:9000

# Create session
SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

# Add HTTP mock
curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{"protocol":"http","match":{"method":"GET","path":"/users/1"},"response":{"status":200,"body":"{\"id\":1,\"name\":\"ada\"}"}}'

# Add Redis mock
curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{"protocol":"redis","match":{"command":"GET","key":"user:1"},"response":"{\"id\":1}"}'

# Verify HTTP mock fires
RESULT=$(curl -sf http://localhost:8080/users/1)
echo "HTTP mock result: $RESULT"

# Verify Redis mock fires
RESULT=$(docker compose exec redis redis-cli -p 6380 GET user:1)
echo "Redis mock result: $RESULT"

# Check status
curl -sf $BASE/status | jq .

# Get traffic
curl -sf $BASE/sessions/$SESSION/traffic | jq .

# Reset session
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Session reset OK"
echo "All smoke tests passed"
```

Save this as `deploy/smoke-test.sh` and run:
```bash
chmod +x deploy/smoke-test.sh && ./deploy/smoke-test.sh
```

Expected: HTTP mock returns `{"id":1,"name":"ada"}`, Redis key exists, traffic shows intercepted calls, session reset cleans up.

- [ ] **Step 3: Run unit tests**

```bash
cd control-plane && go test ./... -v -count=1
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add deploy/smoke-test.sh
git commit -m "test: E2E smoke test script for full Mirage stack"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Session-scoped API (POST/DELETE /sessions) | Task 6 |
| Mock rules CRUD | Task 7 |
| GET /status with per-backend health | Task 8 |
| GET /sessions/:id/traffic | Task 9, 20 |
| BackendAdapter interface | Task 4 |
| WireMock adapter (HTTP) | Task 10 |
| Redis adapter | Task 11 |
| Envoy static config (HTTP + Redis) | Task 12 |
| iptables.sh with UID exclusion | Task 13 |
| docker-compose Phase 1 | Task 14 |
| Env-based config | Task 15 |
| Kafka adapter + Envoy listener | Tasks 16, 17 |
| MySQL adapter + Envoy listener | Tasks 16, 18 |
| RabbitMQ adapter + Envoy listener | Task 19 |
| Near-miss diagnostics | Task 21 |
| network_mode: service:mirage-envoy | Task 14 |

All spec requirements covered. ✓
