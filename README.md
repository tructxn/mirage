# Regista

> Like Andrea Pirlo — sits deep, sees everything, distributes to wherever it needs to go.

Regista is a **network-level mock proxy for E2E testing**. It intercepts all outbound TCP traffic from your service — HTTP, Redis, Kafka, RabbitMQ, MySQL, Oracle — and returns configured fake responses, with zero changes to your production code.

---

## The Problem

E2E tests are supposed to test the whole thing. But most teams end up mocking at the code level anyway:

- You mock the Redis client in your language. Then a Redis-specific behavior you didn't mock breaks in prod.
- You mock the HTTP client per test. The mocks slowly drift from what the real service actually returns.
- You spin up a dozen test containers. Each one is someone's problem to maintain.
- You have no single place to see "what external calls does this service make?"

The root issue: **mocking at the code level is the wrong layer.** It couples your tests to implementation details, not to network behavior.

---

## The Idea

Intercept traffic at the network level instead. Redirect all outbound TCP to a local proxy. The proxy understands each protocol and returns whatever you've configured.

```
Your Service
    │
    │  iptables REDIRECT (per port, transparent)
    ▼
Envoy Sidecar
    │
    ├─ :80 / :443  ──►  HTTP filter     ──►  Control Plane
    ├─ :6379       ──►  redis_proxy     ──►  Control Plane
    ├─ :5672       ──►  tcp_proxy       ──►  Control Plane
    └─ :9092       ──►  kafka_broker    ──►  Control Plane
```

Your service makes calls exactly as it does in production — same DNS names, same ports, same protocol. Regista intercepts them and responds with what you said to respond with.

---

## What You Get

- **Zero code changes** — interception is at the OS network layer via iptables, invisible to your service
- **One API for all protocols** — configure mocks for HTTP, Redis, Kafka, and more from a single REST endpoint
- **Protocol-aware** — Envoy speaks each protocol natively; your service gets a real Redis response, a real HTTP response, not a raw TCP blob
- **Inspectable traffic** — see every outbound call your service made during a test run
- **Resets between tests** — POST `/mocks/reset` and you're clean

---

## Mock Rule Example

```bash
# Mock a Redis GET
POST /mocks
{
  "protocol": "redis",
  "match": { "command": "GET", "key": "user:*" },
  "response": "{\"id\": 1, \"name\": \"ada\"}"
}

# Mock an HTTP call
POST /mocks
{
  "protocol": "http",
  "match": { "method": "POST", "path": "/v1/charge" },
  "response": { "status": 200, "body": { "id": "ch_fake123", "status": "succeeded" } }
}

# Inspect what was called
GET /traffic
```

---

## Architecture

**Envoy** is the proxy — it handles the protocol parsing, matching, and response. It knows Redis RESP, Kafka wire format, HTTP — you don't have to.

**iptables** redirects outbound traffic to Envoy before it leaves the container. Same pattern as Istio/Linkerd service meshes. No proxy env vars, no SDK configuration — it just works.

**Control Plane** is the single API that accepts mock rules, pushes them to the right backend, and exposes traffic inspection.

See [`docs/architecture.md`](docs/architecture.md) for the full breakdown.

---

## Supported Protocols

| Protocol   | Envoy Filter               | Status   |
|------------|----------------------------|----------|
| HTTP/gRPC  | `http_connection_manager`  | Planned  |
| Redis      | `redis_proxy`              | Planned  |
| Kafka      | `kafka_broker`             | Planned  |
| MySQL      | `mysql_proxy`              | Planned  |
| RabbitMQ   | `tcp_proxy` (raw)          | Planned  |
| Oracle     | `tcp_proxy` (raw)          | Planned  |

---

## Inspiration

**Regista** is the Italian football term for a deep-lying playmaker — the Andrea Pirlo role. Sits in front of the defence, reads the whole field, distributes to wherever it needs to go.

This proxy does the same: sits between your service and all external dependencies, directing each call to the right mock.

---

## Prior Art & Alternatives

**Commercial**

- [Traffic Parrot](https://trafficparrot.com/) — closest commercial equivalent. Supports HTTP, Kafka, IBM MQ, TIBCO. Proprietary, license-based, Java-heavy. Good product, but closed and expensive for what it does.
- [Hoverfly](https://hoverfly.io/) — HTTP/HTTPS only, no multi-protocol. SaaS + open core model. Well-maintained but limited to HTTP simulation.
- [WireMock Cloud](https://wiremock.io/) — HTTP mocking as a service. Great for REST/gRPC but doesn't touch Redis, Kafka, or databases.

**Open Source**

- [Grab Loki](https://engineering.grab.com/loki-dynamic-mock-server-http-tcp-testing) — internal tool at Grab, network-intercept approach, inspiration for this project. Not publicly released.
- [Microcks](https://microcks.io/) — strong for Kafka/AsyncAPI and OpenAPI mocking. Complex to operate, not designed for sidecar interception.
- [WireMock](https://wiremock.org/) — de facto standard for HTTP mocking. No network-level interception; requires SDK or proxy env var configuration.

**Where Regista fits**: network-level interception across all protocols, single control plane, open source, designed to run as a sidecar with no service changes.

---

## License

MIT
