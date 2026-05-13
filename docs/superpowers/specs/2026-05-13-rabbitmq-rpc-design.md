# RabbitMQ RPC Mock — System Design

**Date:** 2026-05-13
**Status:** Approved

---

## Problem

The existing `amqp` adapter in Mirage is fire-and-forget: `PushRule` publishes a message to an exchange. This does not cover the RabbitMQ RPC pattern, where a service publishes a request to a queue with `replyTo` + `correlationId` in AMQP properties and expects a response published back to the reply queue.

MoMo services use this pattern extensively — both with named reply queues and with RabbitMQ's direct reply-to pseudo-queue (`amq.rabbitmq.reply-to`).

---

## Goals

- Mock the **server side** of RabbitMQ RPC: consume from a queue, match incoming messages against configured rules, publish response to `replyTo` with same `correlationId`
- Support **direct reply-to** (`amq.rabbitmq.reply-to`) and **named reply queues** transparently (same code path — just publish to whatever `replyTo` contains)
- Match rules by **queue name** (required) + arbitrary **AMQP property and header filters** (optional, AND logic)
- Integrate with existing **session isolation** and **traffic inspection**

---

## Out of Scope

- AMQP body-based matching (headers/properties are sufficient for MoMo patterns)
- Persistent consumers across server restarts (in-memory only)
- TLS AMQP connections

---

## Protocol

A new `"amqp-rpc"` protocol, separate from the existing `"amqp"` (fire-and-forget) protocol. Both use the same `RABBITMQ_URL`.

---

## Rule Format

```json
POST /sessions/:id/mocks
{
  "protocol": "amqp-rpc",
  "match": {
    "queue": "profile_service",
    "properties.type": "GET_PROFILE",
    "headers.X-Source": "payment-service"
  },
  "response": "{\"userId\": 1, \"name\": \"ada\"}",
  "priority": 1
}
```

### Match fields

| Field | Required | Description |
|---|---|---|
| `queue` | Yes | Queue to consume from |
| `properties.type` | No | AMQP `type` BasicProperty |
| `properties.content_type` | No | AMQP `content_type` BasicProperty |
| `headers.<name>` | No | Any key in the AMQP headers table |

All specified fields must match (AND logic). Rules are evaluated in descending priority order; first match wins.

### Response

- Published to `delivery.ReplyTo` (works for both `amq.rabbitmq.reply-to` and named queues)
- `correlationId` = same as incoming `delivery.CorrelationId`
- Body = `rule.Response` serialized as string
- Content-type = `application/json`

---

## Architecture

```
Test client
  POST /sessions/:id/mocks  (protocol: "amqp-rpc")
       ↓
Control Plane
  RabbitMQRPCAdapter.PushRule()
    → register rule
    → start consumer goroutine on match.queue (if not already running)

Service under test
  → publishes to "profile_service" with replyTo + correlationId
       ↓
RabbitMQ broker (rabbitmq container)
       ↓
Consumer goroutine in RabbitMQRPCAdapter
  → match against session rules
  → publish response to replyTo with same correlationId
  → record TrafficRecord
       ↓
Service under test receives response
```

---

## Internal Structure

### Files

- `control-plane/adapters/rabbitmq_rpc.go` — new adapter
- `control-plane/adapters/rabbitmq_rpc_test.go` — unit tests (no live broker)
- `control-plane/main.go` — register `"amqp-rpc"` with same `RABBITMQ_URL`

### Data structures

```go
type RabbitMQRPCAdapter struct {
    url      string
    mu       sync.Mutex
    sessions map[string]*rpcSession
}

type rpcSession struct {
    rules     []session.Rule
    consumers map[string]context.CancelFunc  // queue → goroutine cancel
    traffic   []session.TrafficRecord
}
```

### Consumer goroutine

One goroutine per (sessionID, queue) pair:

1. Open dedicated AMQP connection + channel
2. `ch.Consume(queue, ...)` — receive deliveries
3. For each delivery:
   - Run `matchRule(sessionRules, delivery)` — evaluate rules for this queue, sorted by priority, first full match wins
   - If match: publish response to `delivery.ReplyTo` with same `delivery.CorrelationId`; record `TrafficRecord{MatchedRuleID: &rule.ID}`
   - If no match: record `TrafficRecord{NearMiss: &NearMiss{RuleID: closestRuleID, FailedField: "..."}}`
   - Ack delivery either way
4. On context cancel: stop loop, close connection

### Matching function (pure, testable)

```go
func matchRule(rules []session.Rule, d amqp.Delivery) (session.Rule, bool)
```

For each rule (sorted by priority desc):
- `match.queue` is already pre-filtered by consumer goroutine
- `properties.type` → compare against `d.Type`
- `properties.content_type` → compare against `d.ContentType`
- `headers.<name>` → look up key in `d.Headers` table, compare string value
- All specified fields must equal the delivery value (exact match)

### BackendAdapter interface compliance

| Method | Behavior |
|---|---|
| `PushRule` | Register rule, start consumer goroutine if needed. Returns error if `match.queue` missing. |
| `DeleteRule` | Remove rule. Cancel consumer if no more rules for that queue in session. |
| `Reset` | Cancel all consumers for session, clear rules and traffic. |
| `Traffic` | Return recorded `TrafficRecord` slice for session. |
| `Healthy` | Dial AMQP with 2s timeout, same as existing adapter. |

---

## Session Isolation

Each session has its own `rpcSession` with independent rules, consumers, and traffic log. `Reset` (DELETE /sessions/:id) cancels all consumers and clears state for that session only. Multiple parallel test sessions consume from the same broker but match only against their own rules.

---

## Traffic Inspection

`GET /sessions/:id/traffic` returns RPC calls with:

```json
{
  "timestamp": "...",
  "protocol": "amqp-rpc",
  "request": {
    "queue": "profile_service",
    "correlation_id": "550e8400-...",
    "reply_to": "amq.rabbitmq.reply-to",
    "properties.type": "GET_PROFILE",
    "headers": { "X-Source": "payment-service" }
  },
  "matched_rule_id": "rule-uuid",
  "response": "{\"userId\": 1}",
  "duration_ms": 3
}
```

For unmatched: `matched_rule_id` is null, `near_miss` contains closest rule ID and failed field.

---

## Testing Plan

All tests run without a live broker by testing the matching logic in isolation:

| Test | Verifies |
|---|---|
| `TestRPCPushRuleMissingQueue` | Returns error when `match.queue` is absent |
| `TestRPCMatchByQueue` | Message on correct queue with no other filters matches |
| `TestRPCMatchByPropertyType` | `properties.type` filter works |
| `TestRPCMatchByHeader` | `headers.X-Source` filter works |
| `TestRPCMatchAllFilters` | Multiple filters all required (AND logic) |
| `TestRPCMatchPriority` | Higher priority rule wins over lower priority |
| `TestRPCNoMatch` | No matching rule → no match returned |
| `TestRPCHealthyUnreachable` | `Healthy()` returns false for unreachable broker |
