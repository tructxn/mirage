# RabbitMQ RPC Mock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `"amqp-rpc"` protocol adapter that consumes from a queue, matches incoming messages against configured rules, and publishes responses to `replyTo` with the same `correlationId` — mocking the server side of RabbitMQ RPC.

**Architecture:** A new `RabbitMQRPCAdapter` (separate from the existing fire-and-forget `RabbitMQAdapter`) manages per-session consumer goroutines. `PushRule` registers a rule and starts a consumer goroutine on `match.queue` if one isn't already running. Each incoming delivery is matched against rules by queue + optional AMQP property/header filters. The response is published to `delivery.ReplyTo` with the original `correlationId`. Traffic is recorded per session.

**Tech Stack:** Go 1.22, `github.com/rabbitmq/amqp091-go` (already in go.mod), `context` for goroutine cancellation, `sync.Mutex` for shared state.

---

## File Map

```
control-plane/adapters/
├── rabbitmq_rpc.go        — RabbitMQRPCAdapter, rpcSession, matchRule, ruleMatches, nearMissField, runConsumer, handleDelivery
└── rabbitmq_rpc_test.go   — white-box unit tests for matchRule + PushRule validation (package adapters, no live broker)

control-plane/main.go      — register "amqp-rpc" adapter with same RABBITMQ_URL
```

No other files change.

---

## Task 1: Write failing tests

**Files:**
- Create: `control-plane/adapters/rabbitmq_rpc_test.go`

Note: this test file uses `package adapters` (white-box, not `package adapters_test`) so it can call the unexported `matchRule` function. This is valid Go — both package styles can coexist in one directory.

- [ ] **Step 1: Create the test file**

```go
// control-plane/adapters/rabbitmq_rpc_test.go
package adapters

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

// --- matchRule tests (pure function, no network) ---

func rules(rs ...session.Rule) []session.Rule { return rs }

func rule(id string, priority int, match map[string]any) session.Rule {
	return session.Rule{ID: id, Protocol: "amqp-rpc", Match: match, Response: `{"ok":true}`, Priority: priority}
}

func delivery(typ, contentType string, headers amqp.Table) amqp.Delivery {
	return amqp.Delivery{
		Type:        typ,
		ContentType: contentType,
		Headers:     headers,
		ReplyTo:     "amq.rabbitmq.reply-to",
		CorrelationId: "corr-123",
	}
}

func TestMatchRuleByQueue(t *testing.T) {
	// queue filtering happens before matchRule — rules passed in are already for this queue
	r := rule("r1", 0, map[string]any{"queue": "profile_service"})
	d := delivery("", "", nil)
	got, ok := matchRule(rules(r), d)
	if !ok || got.ID != "r1" {
		t.Fatalf("expected match r1, got ok=%v id=%s", ok, got.ID)
	}
}

func TestMatchRuleByPropertyType(t *testing.T) {
	r := rule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := delivery("GET_PROFILE", "", nil)
	_, ok := matchRule(rules(r), d)
	if !ok {
		t.Fatal("expected match on properties.type")
	}
}

func TestMatchRuleByPropertyTypeMiss(t *testing.T) {
	r := rule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := delivery("OTHER_TYPE", "", nil)
	_, ok := matchRule(rules(r), d)
	if ok {
		t.Fatal("expected no match when properties.type differs")
	}
}

func TestMatchRuleByHeader(t *testing.T) {
	r := rule("r1", 0, map[string]any{"queue": "q", "headers.X-Source": "payment-service"})
	d := delivery("", "", amqp.Table{"X-Source": "payment-service"})
	_, ok := matchRule(rules(r), d)
	if !ok {
		t.Fatal("expected match on header")
	}
}

func TestMatchRuleByHeaderMiss(t *testing.T) {
	r := rule("r1", 0, map[string]any{"queue": "q", "headers.X-Source": "payment-service"})
	d := delivery("", "", amqp.Table{"X-Source": "other-service"})
	_, ok := matchRule(rules(r), d)
	if ok {
		t.Fatal("expected no match when header value differs")
	}
}

func TestMatchRuleAllFilters(t *testing.T) {
	r := rule("r1", 0, map[string]any{
		"queue":           "q",
		"properties.type": "GET_PROFILE",
		"headers.X-Source": "payment-service",
	})
	// all match
	d := delivery("GET_PROFILE", "", amqp.Table{"X-Source": "payment-service"})
	_, ok := matchRule(rules(r), d)
	if !ok {
		t.Fatal("expected match when all filters satisfied")
	}
	// one misses
	d2 := delivery("GET_PROFILE", "", amqp.Table{"X-Source": "wrong"})
	_, ok2 := matchRule(rules(r), d2)
	if ok2 {
		t.Fatal("expected no match when one filter fails")
	}
}

func TestMatchRulePriority(t *testing.T) {
	low := rule("low", 1, map[string]any{"queue": "q"})
	high := rule("high", 10, map[string]any{"queue": "q"})
	d := delivery("", "", nil)
	got, ok := matchRule(rules(low, high), d)
	if !ok || got.ID != "high" {
		t.Fatalf("expected high-priority rule to win, got id=%s ok=%v", got.ID, ok)
	}
}

func TestMatchRuleNoMatch(t *testing.T) {
	r := rule("r1", 0, map[string]any{"queue": "q", "properties.type": "GET_PROFILE"})
	d := delivery("OTHER", "", nil)
	_, ok := matchRule(rules(r), d)
	if ok {
		t.Fatal("expected no match")
	}
}

func TestMatchRuleEmptyRules(t *testing.T) {
	d := delivery("GET_PROFILE", "", nil)
	_, ok := matchRule(nil, d)
	if ok {
		t.Fatal("expected no match for empty rules")
	}
}

// --- PushRule validation test ---

func TestRPCPushRuleMissingQueue(t *testing.T) {
	a := NewRabbitMQRPCAdapter("amqp://guest:guest@localhost:19999/")
	rule := &session.Rule{
		ID:       "r1",
		Protocol: "amqp-rpc",
		Match:    map[string]any{}, // no queue
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
```

- [ ] **Step 2: Run to verify compile error**

```bash
cd /path/to/control-plane && go test ./adapters/... -run TestMatch -v 2>&1 | head -10
```

Expected: compile error — `matchRule undefined`, `NewRabbitMQRPCAdapter undefined`.

---

## Task 2: Implement the RPC adapter

**Files:**
- Create: `control-plane/adapters/rabbitmq_rpc.go`

- [ ] **Step 1: Create the implementation file**

```go
// control-plane/adapters/rabbitmq_rpc.go
package adapters

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

// RabbitMQRPCAdapter mocks the server side of RabbitMQ RPC.
// It consumes from configured queues and replies to replyTo with the configured response.
type RabbitMQRPCAdapter struct {
	url      string
	mu       sync.Mutex
	sessions map[string]*rpcSession
}

type rpcSession struct {
	rules     []session.Rule
	consumers map[string]context.CancelFunc // queue → goroutine cancel func
	traffic   []session.TrafficRecord
}

func NewRabbitMQRPCAdapter(url string) *RabbitMQRPCAdapter {
	return &RabbitMQRPCAdapter{
		url:      url,
		sessions: make(map[string]*rpcSession),
	}
}

func (a *RabbitMQRPCAdapter) getOrCreateSession(sessionID string) *rpcSession {
	sess, ok := a.sessions[sessionID]
	if !ok {
		sess = &rpcSession{consumers: make(map[string]context.CancelFunc)}
		a.sessions[sessionID] = sess
	}
	return sess
}

// PushRule registers the rule and starts a consumer goroutine on match.queue
// if one is not already running for this session+queue.
func (a *RabbitMQRPCAdapter) PushRule(sessionID string, rule *session.Rule) error {
	queue, ok := rule.Match["queue"].(string)
	if !ok || queue == "" {
		return fmt.Errorf("amqp-rpc rule missing match.queue")
	}

	a.mu.Lock()
	sess := a.getOrCreateSession(sessionID)
	sess.rules = append(sess.rules, *rule)
	_, alreadyConsuming := sess.consumers[queue]
	if !alreadyConsuming {
		ctx, cancel := context.WithCancel(context.Background())
		sess.consumers[queue] = cancel
	}
	a.mu.Unlock()

	if !alreadyConsuming {
		go a.runConsumer(context.Background(), sessionID, queue)
		// Note: we stored the cancel but pass a fresh ctx to runConsumer.
		// Re-acquire cancel from session in runConsumer so it obeys Reset().
	}
	return nil
}
```

Wait — there's a subtle bug above: we store cancel but pass `context.Background()` to `runConsumer`. Fix: pass the derived ctx directly. Revise:

```go
// control-plane/adapters/rabbitmq_rpc.go
package adapters

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tructxn/mirage/control-plane/session"
)

type RabbitMQRPCAdapter struct {
	url      string
	mu       sync.Mutex
	sessions map[string]*rpcSession
}

type rpcSession struct {
	rules     []session.Rule
	consumers map[string]context.CancelFunc
	traffic   []session.TrafficRecord
}

func NewRabbitMQRPCAdapter(url string) *RabbitMQRPCAdapter {
	return &RabbitMQRPCAdapter{
		url:      url,
		sessions: make(map[string]*rpcSession),
	}
}

func (a *RabbitMQRPCAdapter) getOrCreateSession(sessionID string) *rpcSession {
	sess, ok := a.sessions[sessionID]
	if !ok {
		sess = &rpcSession{consumers: make(map[string]context.CancelFunc)}
		a.sessions[sessionID] = sess
	}
	return sess
}

func (a *RabbitMQRPCAdapter) PushRule(sessionID string, rule *session.Rule) error {
	queue, ok := rule.Match["queue"].(string)
	if !ok || queue == "" {
		return fmt.Errorf("amqp-rpc rule missing match.queue")
	}

	a.mu.Lock()
	sess := a.getOrCreateSession(sessionID)
	sess.rules = append(sess.rules, *rule)
	_, alreadyConsuming := sess.consumers[queue]
	var ctx context.Context
	var cancel context.CancelFunc
	if !alreadyConsuming {
		ctx, cancel = context.WithCancel(context.Background())
		sess.consumers[queue] = cancel
	}
	a.mu.Unlock()

	if !alreadyConsuming {
		go a.runConsumer(ctx, sessionID, queue)
	}
	return nil
}

func (a *RabbitMQRPCAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	queue, _ := rule.Match["queue"].(string)

	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil
	}
	filtered := sess.rules[:0]
	for _, r := range sess.rules {
		if r.ID != rule.ID {
			filtered = append(filtered, r)
		}
	}
	sess.rules = filtered

	if queue != "" {
		hasMore := false
		for _, r := range sess.rules {
			if q, _ := r.Match["queue"].(string); q == queue {
				hasMore = true
				break
			}
		}
		if !hasMore {
			if cancel, ok := sess.consumers[queue]; ok {
				cancel()
				delete(sess.consumers, queue)
			}
		}
	}
	return nil
}

func (a *RabbitMQRPCAdapter) Reset(sessionID string, rules []session.Rule) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil
	}
	for _, cancel := range sess.consumers {
		cancel()
	}
	delete(a.sessions, sessionID)
	return nil
}

func (a *RabbitMQRPCAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sess, ok := a.sessions[sessionID]
	if !ok {
		return nil, nil
	}
	out := make([]session.TrafficRecord, len(sess.traffic))
	copy(out, sess.traffic)
	return out, nil
}

func (a *RabbitMQRPCAdapter) Healthy() bool {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// matchRule evaluates rules against an incoming delivery.
// rules should already be filtered to those matching the consumer's queue.
// Returns the highest-priority matching rule, or zero value + false if none match.
func matchRule(rules []session.Rule, d amqp.Delivery) (session.Rule, bool) {
	sorted := make([]session.Rule, len(rules))
	copy(sorted, rules)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Priority > sorted[j-1].Priority; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	for _, rule := range sorted {
		if ruleMatches(rule, d) {
			return rule, true
		}
	}
	return session.Rule{}, false
}

// ruleMatches returns true if all non-queue match fields in rule match the delivery.
func ruleMatches(rule session.Rule, d amqp.Delivery) bool {
	for k, v := range rule.Match {
		if k == "queue" {
			continue
		}
		expected, _ := v.(string)
		var actual string
		switch {
		case k == "properties.type":
			actual = d.Type
		case k == "properties.content_type":
			actual = d.ContentType
		case strings.HasPrefix(k, "headers."):
			headerKey := strings.TrimPrefix(k, "headers.")
			if hv, ok := d.Headers[headerKey]; ok {
				actual = fmt.Sprintf("%v", hv)
			}
		default:
			continue // unknown field — ignore
		}
		if actual != expected {
			return false
		}
	}
	return true
}

// nearMissField returns the first match field that fails for the given rule and delivery.
func nearMissField(rule session.Rule, d amqp.Delivery) string {
	for k, v := range rule.Match {
		if k == "queue" {
			continue
		}
		expected, _ := v.(string)
		var actual string
		switch {
		case k == "properties.type":
			actual = d.Type
		case k == "properties.content_type":
			actual = d.ContentType
		case strings.HasPrefix(k, "headers."):
			headerKey := strings.TrimPrefix(k, "headers.")
			if hv, ok := d.Headers[headerKey]; ok {
				actual = fmt.Sprintf("%v", hv)
			}
		}
		if actual != expected {
			return k
		}
	}
	return "unknown"
}

func (a *RabbitMQRPCAdapter) runConsumer(ctx context.Context, sessionID, queue string) {
	conn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			a.handleDelivery(sessionID, queue, d)
		}
	}
}

func (a *RabbitMQRPCAdapter) handleDelivery(sessionID, queue string, d amqp.Delivery) {
	start := time.Now()

	a.mu.Lock()
	var queueRules []session.Rule
	if sess, ok := a.sessions[sessionID]; ok {
		for _, r := range sess.rules {
			if q, _ := r.Match["queue"].(string); q == queue {
				queueRules = append(queueRules, r)
			}
		}
	}
	a.mu.Unlock()

	reqMap := map[string]any{
		"queue":          queue,
		"correlation_id": d.CorrelationId,
		"reply_to":       d.ReplyTo,
	}
	if d.Type != "" {
		reqMap["properties.type"] = d.Type
	}
	if len(d.Headers) > 0 {
		headers := map[string]string{}
		for k, v := range d.Headers {
			headers[k] = fmt.Sprintf("%v", v)
		}
		reqMap["headers"] = headers
	}

	rec := session.TrafficRecord{
		Timestamp: time.Now().UTC(),
		Protocol:  "amqp-rpc",
		Request:   reqMap,
	}

	matched, found := matchRule(queueRules, d)
	if found {
		body := fmt.Sprintf("%v", matched.Response)
		if d.ReplyTo != "" {
			replyConn, err := amqp.DialConfig(a.url, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
			if err == nil {
				replyCh, err := replyConn.Channel()
				if err == nil {
					_ = replyCh.Publish("", d.ReplyTo, false, false, amqp.Publishing{
						ContentType:   "application/json",
						CorrelationId: d.CorrelationId,
						Body:          []byte(body),
					})
					replyCh.Close()
				}
				replyConn.Close()
			}
		}
		id := matched.ID
		rec.MatchedRuleID = &id
		rec.Response = body
	} else if len(queueRules) > 0 {
		closest := queueRules[0]
		rec.NearMiss = &session.NearMiss{
			RuleID:      closest.ID,
			FailedField: nearMissField(closest, d),
		}
	}

	rec.DurationMS = time.Since(start).Milliseconds()
	_ = d.Ack(false)

	a.mu.Lock()
	if sess, ok := a.sessions[sessionID]; ok {
		sess.traffic = append(sess.traffic, rec)
	}
	a.mu.Unlock()
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /path/to/control-plane && go test ./adapters/... -run "TestMatch|TestRPC" -v
```

Expected output:
```
--- PASS: TestMatchRuleByQueue
--- PASS: TestMatchRuleByPropertyType
--- PASS: TestMatchRuleByPropertyTypeMiss
--- PASS: TestMatchRuleByHeader
--- PASS: TestMatchRuleByHeaderMiss
--- PASS: TestMatchRuleAllFilters
--- PASS: TestMatchRulePriority
--- PASS: TestMatchRuleNoMatch
--- PASS: TestMatchRuleEmptyRules
--- PASS: TestRPCPushRuleMissingQueue
--- PASS: TestRPCHealthyUnreachable
PASS
```

- [ ] **Step 3: Run the full test suite to confirm nothing broke**

```bash
cd /path/to/control-plane && go test ./... -count=1
```

Expected: all existing tests still PASS.

---

## Task 3: Register in main.go and commit

**Files:**
- Modify: `control-plane/main.go`

Current `main.go` ends with:
```go
mysqlDSN := getenv("MYSQL_DSN", "root:mirage@tcp(mysql:3307)/mirage")
registry.Register("mysql", adapters.NewMySQLAdapter(mysqlDSN))

srv := api.NewServer(store, registry)
```

- [ ] **Step 1: Add the amqp-rpc registration**

Add after the mysql line, before `srv := api.NewServer(...)`:

```go
registry.Register("amqp-rpc", adapters.NewRabbitMQRPCAdapter(rabbitURL))
```

`rabbitURL` is already defined above (used by the `amqp` adapter). The full block becomes:

```go
rabbitURL := getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5673/")
registry.Register("amqp", adapters.NewRabbitMQAdapter(rabbitURL))
registry.Register("amqp-rpc", adapters.NewRabbitMQRPCAdapter(rabbitURL))
```

- [ ] **Step 2: Build to confirm no compile errors**

```bash
cd /path/to/control-plane && go build ./...
```

Expected: no output (success).

- [ ] **Step 3: Run all tests**

```bash
cd /path/to/control-plane && go test ./... -count=1
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add control-plane/adapters/rabbitmq_rpc.go control-plane/adapters/rabbitmq_rpc_test.go control-plane/main.go
git commit -m "feat: RabbitMQ RPC adapter — amqp-rpc protocol with consumer goroutines and near-miss diagnostics"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| New `"amqp-rpc"` protocol, separate from `"amqp"` | Task 3 (main.go registration) |
| `match.queue` required, error if missing | Task 2 (PushRule validation) + Task 1 (TestRPCPushRuleMissingQueue) |
| `properties.type`, `properties.content_type` filters | Task 2 (ruleMatches) + Task 1 (TestMatchRuleByPropertyType) |
| `headers.<name>` filters | Task 2 (ruleMatches) + Task 1 (TestMatchRuleByHeader) |
| AND logic — all filters must match | Task 2 (ruleMatches) + Task 1 (TestMatchRuleAllFilters) |
| Priority ordering — highest wins | Task 2 (matchRule sort) + Task 1 (TestMatchRulePriority) |
| Response published to `replyTo` with same `correlationId` | Task 2 (handleDelivery) |
| Works for both `amq.rabbitmq.reply-to` and named queues | Task 2 (handleDelivery — no special casing, just publishes to `d.ReplyTo`) |
| Consumer goroutine per session+queue, started on PushRule | Task 2 (PushRule + runConsumer) |
| Consumer cancelled on DeleteRule (last rule for queue) | Task 2 (DeleteRule) |
| All consumers cancelled on Reset | Task 2 (Reset) |
| TrafficRecord with MatchedRuleID | Task 2 (handleDelivery) |
| NearMiss with closest rule + failed field | Task 2 (handleDelivery + nearMissField) |
| `Traffic()` returns session traffic log | Task 2 (Traffic method) |
| `Healthy()` — dial check | Task 2 + Task 1 (TestRPCHealthyUnreachable) |
| Same `RABBITMQ_URL` env var | Task 3 (reuses rabbitURL variable) |

All requirements covered. ✓

**Placeholder scan:** No TBDs, no "implement later". ✓

**Type consistency:** `matchRule([]session.Rule, amqp.Delivery) (session.Rule, bool)` used consistently in implementation and tests. `rpcSession` struct matches usage in PushRule/DeleteRule/Reset/Traffic. ✓
