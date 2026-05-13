# Mirage — System Design

**Date:** 2026-05-13
**Status:** Approved

---

## Problem

Modern services talk to many external dependencies: Redis, Kafka, HTTP APIs, MySQL, RabbitMQ. Running E2E tests requires either spinning up all those real dependencies (expensive, slow, flaky) or mocking at the code level (brittle, language-specific, drifts from real behavior).

Mirage solves this by intercepting all outbound TCP at the network layer and returning configured fake responses — with zero changes to the service under test.

---

## Goals

- Run your service in Docker, mock everything outside it
- No code changes to the service under test
- One REST API to configure mocks for all protocols
- Session-scoped isolation — parallel tests don't stomp each other
- Traffic inspection with match diagnostics after each test

---

## Architecture

```
docker-compose
  ├─ your-service               (unchanged — real code, real logic)
  ├─ mirage-envoy               (iptables redirect + Envoy proxy)
  ├─ mirage-control-plane :9000 (Go — rules API, sessions, traffic)
  ├─ wiremock              :8080 (HTTP/gRPC mocks)
  ├─ miniredis             :6380 (Redis mocks)
  ├─ kafka-mock            :9093 (Kafka mocks)
  ├─ mysql-mock            :3307 (MySQL mocks)
  └─ rabbitmq-mock         :5673 (AMQP mocks)
```

**Traffic flow:**
```
your-service
    ↓  iptables REDIRECT (transparent, per port)
Envoy sidecar
    ├─ :15001 (←:80)    → http_connection_manager → wiremock:8080
    ├─ :15002 (←:443)   → http_connection_manager → wiremock:8080
    ├─ :15003 (←:6379)  → redis_proxy             → miniredis:6380
    ├─ :15004 (←:9092)  → kafka_broker            → kafka-mock:9093
    ├─ :15005 (←:3306)  → mysql_proxy             → mysql-mock:3307
    └─ :15006 (←:5672)  → tcp_proxy               → rabbitmq-mock:5673
```

The control plane sits **beside** Envoy — not in the traffic path. It pushes rules to backends via their native APIs and pulls traffic records from them for inspection.

---

## Components

### 1. iptables (`envoy/iptables.sh`)

Runs as init script before Envoy starts. Redirects outbound traffic per port to Envoy listeners. Excludes Envoy's own UID (1337) to prevent redirect loops.

```bash
iptables -t nat -A OUTPUT -m owner --uid-owner 1337 -j RETURN
iptables -t nat -A OUTPUT -p tcp --dport 80   -j REDIRECT --to-port 15001
iptables -t nat -A OUTPUT -p tcp --dport 443  -j REDIRECT --to-port 15002
iptables -t nat -A OUTPUT -p tcp --dport 6379 -j REDIRECT --to-port 15003
iptables -t nat -A OUTPUT -p tcp --dport 9092 -j REDIRECT --to-port 15004
iptables -t nat -A OUTPUT -p tcp --dport 3306 -j REDIRECT --to-port 15005
iptables -t nat -A OUTPUT -p tcp --dport 5672 -j REDIRECT --to-port 15006
```

In docker-compose, `your-service` uses `network_mode: service:mirage-envoy` to share Envoy's network namespace. This means iptables rules set in Envoy's init apply to both containers — all outbound traffic from `your-service` is redirected through Envoy's listeners. `mirage-envoy` requires `cap_add: NET_ADMIN`; `your-service` does not.

### 2. Envoy (`envoy/envoy.yaml`)

Static configuration — no xDS for V1. Each listener maps a port to a protocol filter and upstream cluster. Restart container to apply config changes.

| Listener | Filter | Upstream |
|----------|--------|----------|
| :15001, :15002 | `http_connection_manager` | wiremock:8080 |
| :15003 | `redis_proxy` | miniredis:6380 |
| :15004 | `kafka_broker` | kafka-mock:9093 |
| :15005 | `mysql_proxy` | mysql-mock:3307 |
| :15006 | `tcp_proxy` | rabbitmq-mock:5673 |

### 3. Control Plane (`control-plane/`)

Go binary. Single responsibility: manage mock rules per session and expose traffic inspection.

**Internal structure:**
```
control-plane/
├── main.go
├── api/          — HTTP handlers
├── session/      — in-memory session store
├── adapters/
│   ├── adapter.go       — BackendAdapter interface
│   ├── wiremock.go
│   ├── miniredis.go
│   ├── kafka.go
│   ├── mysql.go
│   └── rabbitmq.go
└── go.mod
```

**BackendAdapter interface:**
```go
type BackendAdapter interface {
    PushRule(sessionID string, rule Rule) error
    DeleteRule(sessionID string, ruleID string) error
    Reset(sessionID string) error
    Traffic(sessionID string) ([]TrafficRecord, error)
}
```

Each adapter translates the universal mock rule format into the backend's native API calls.

---

## API

### Sessions

```
POST   /sessions              → { "id": "uuid" }
DELETE /sessions/:id          → resets all rules + clears traffic for session
```

### Mock Rules

```
POST   /sessions/:id/mocks              → { "id": "rule-uuid" }
GET    /sessions/:id/mocks              → [ ...rules ]
DELETE /sessions/:id/mocks/:ruleId
```

**Rule body:**
```json
{
  "protocol": "redis|http|kafka|mysql|amqp",
  "match": { ... },
  "response": { ... },
  "priority": 1
}
```

Protocol-specific match shapes:

| Protocol | Match fields |
|----------|-------------|
| http | `method`, `path`, `headers`, `body` |
| redis | `command`, `key` |
| kafka | `topic`, `key`, `value` |
| mysql | `query` (regex) |
| amqp | `exchange`, `routing_key` |

### Traffic Inspection

```
GET /sessions/:id/traffic
```

Response per intercepted call:
```json
{
  "timestamp": "...",
  "protocol": "redis",
  "request": { "command": "GET", "key": "user:42" },
  "matched_rule_id": "rule-uuid",
  "response": "{ \"id\": 1 }",
  "duration_ms": 2
}
```

For unmatched requests: `matched_rule_id` is null, `near_miss` contains the closest rule and which field failed.

### Status

```
GET /status
```

Returns per-backend: reachable (bool), last check timestamp.

---

## Session Isolation

Sessions scope rules. When a test resets (`DELETE /sessions/:id`), only that session's rules are removed from the backend. Parallel isolation is best-effort — for full parallel safety, run separate Mirage instances per parallel test worker. Sequential test runs are fully isolated.

---

## Folder Structure

```
mirage/
├── control-plane/
│   ├── main.go
│   ├── api/
│   ├── session/
│   ├── adapters/
│   └── go.mod
├── envoy/
│   ├── envoy.yaml
│   └── iptables.sh
├── deploy/
│   └── docker-compose.yml
└── docs/
    ├── architecture.md
    └── superpowers/specs/
```

---

## Implementation Phases

### Phase 1 — HTTP + Redis (core loop)
- Control plane API (sessions + mocks + status)
- WireMock adapter + miniredis adapter
- Envoy static config for HTTP + Redis listeners
- iptables script
- docker-compose wiring all together
- Manual E2E test: real Go service → Mirage → mocked HTTP + Redis responses

### Phase 2 — Kafka + MySQL
- kafka-mock container + Kafka adapter
- mysql-mock container + MySQL adapter
- Envoy listeners for Kafka + MySQL

### Phase 3 — RabbitMQ
- rabbitmq-mock container + AMQP adapter
- Envoy tcp_proxy listener for AMQP

### Phase 4 — Traffic + Diagnostics
- Full `GET /sessions/:id/traffic` implementation
- Near-miss diagnostics pulled from each backend
- Per-backend traffic normalization into unified TrafficRecord format

---

## Out of Scope (V1)

- Oracle support (TestContainers Oracle, deferred)
- xDS / dynamic Envoy config
- Client SDKs (raw HTTP API first)
- TLS interception
- Metrics / Prometheus endpoint
