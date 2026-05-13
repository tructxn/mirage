# Regista

> Like Andrea Pirlo — sits deep, sees everything, distributes to wherever it needs to go.

Regista is a **network-level mock proxy for E2E testing**. Run your service in Docker against mocked external dependencies — no real Redis cluster, no real Kafka broker, no real payment gateway. Your service runs its real code. Everything outside it is intercepted and faked.

---

## The Problem

Modern services talk to a lot of things: Redis, Kafka, HTTP APIs, MySQL, RabbitMQ. Running a full E2E test means either spinning up all of those dependencies (expensive, slow, flaky) or mocking them at the code level (brittle, language-specific, drifts from real behavior).

Both approaches share the same root problem: **you're managing the wrong thing.**

- Spinning up real clusters: you're maintaining test infrastructure that mirrors production. It breaks independently of your code.
- Code-level mocks: you're mocking the client library, not the network. Your mock of `redis.get()` has nothing to do with what a real Redis server actually returns.

What you actually want: **run your service for real, fake everything outside it, and have a single place to control what those fakes return.**

---

## The Idea

Redirect all outbound TCP from your service to a local proxy. The proxy understands each protocol and responds with whatever you've configured. Your service makes calls exactly as it does in production — same DNS, same ports, same wire protocol. It never knows it's talking to a fake.

```
docker-compose up
  ├─ your-service          ← real code, real logic, no changes
  └─ regista
       ├─ :80 / :443  ──►  HTTP/gRPC  ──►  mocked
       ├─ :6379       ──►  Redis       ──►  mocked
       ├─ :9092       ──►  Kafka       ──►  mocked
       ├─ :3306       ──►  MySQL       ──►  mocked
       └─ :5672       ──►  RabbitMQ   ──►  mocked
```

No Redis container. No Kafka broker. No external sandbox accounts. Just your service and Regista.

---

## How It Works

**iptables** intercepts all outbound TCP before it leaves the container — transparent, no env vars, no SDK config, no changes to your service.

**Envoy** receives the traffic and parses it natively — it speaks Redis RESP, Kafka wire protocol, HTTP/2, MySQL protocol. Your service gets a real protocol response back, not a raw TCP blob.

**Control Plane** is the single API you use from your tests to configure what Regista returns.

---

## Using It From Tests

```bash
# Before your test: set up mocks
POST /sessions/test-123/mocks
{
  "protocol": "redis",
  "match": { "command": "GET", "key": "user:*" },
  "response": "{\"id\": 1, \"name\": \"ada\"}"
}

POST /sessions/test-123/mocks
{
  "protocol": "http",
  "match": { "method": "POST", "path": "/v1/charge" },
  "response": { "status": 200, "body": { "id": "ch_fake123", "status": "succeeded" } }
}

# Run your test ...

# After: inspect what your service actually called
GET /sessions/test-123/traffic
# → every intercepted call, which rule matched, what was returned

# Reset for next test
DELETE /sessions/test-123
```

Mocks are **session-scoped** — parallel tests don't stomp on each other. Each test gets its own session ID.

The `/traffic` response shows exactly what was intercepted: the raw request, which rule it matched against, and why. No more guessing why a mock didn't fire.

---

## Supported Protocols

| Protocol   | Envoy Filter               | Backend              | Status  |
|------------|----------------------------|----------------------|---------|
| HTTP/gRPC  | `http_connection_manager`  | WireMock / built-in  | Planned |
| Redis      | `redis_proxy`              | miniredis            | Planned |
| Kafka      | `kafka_broker`             | Microcks AsyncAPI    | Planned |
| MySQL      | `mysql_proxy`              | test container       | Planned |
| RabbitMQ   | `tcp_proxy` (raw)          | rabbitmq-mock        | Planned |
| Oracle     | `tcp_proxy` (raw)          | TestContainers       | Planned |

---

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full breakdown.

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

**Where Regista fits**: network-level interception across all protocols, single control plane, session-scoped isolation, open source, designed to run as a Docker sidecar with no service changes.

---

## Inspiration

**Regista** is the Italian football term for a deep-lying playmaker — the Andrea Pirlo role. Sits in front of the defence, reads the whole field, distributes to wherever it needs to go.

This proxy does the same: sits between your service and all external dependencies, directing each call to the right mock.

---

## License

MIT
