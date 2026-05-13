# Regista

> Like Andrea Pirlo — sits deep, sees everything, distributes to wherever it needs to go.

**Regista** is a universal mock proxy for E2E testing. Instead of mocking at the code level, it intercepts all outbound TCP traffic (HTTP, Redis, RabbitMQ, Kafka, Oracle, and more) at the network level via Envoy and returns configured fake responses from a single control plane.

## The Problem

Traditional E2E tests mock at the code level:
- Language-specific mock libraries per dependency
- Mocks drift from real behavior over time
- No single source of truth for "what external calls does this service make"

## The Idea

Redirect **all** outbound TCP to a localhost proxy. The proxy understands each protocol and returns configured fake responses. Zero changes to production code.

```
[Your Service]
      │
      │  iptables REDIRECT (per port)
      ▼
[Envoy Sidecar]
      │
      ├─ port 80/443  ──► HTTP filter   ──► Mock Control Plane
      ├─ port 6379    ──► redis_proxy   ──► Mock Control Plane
      ├─ port 5672    ──► tcp_proxy     ──► Mock Control Plane
      └─ port 9092    ──► kafka_broker  ──► Mock Control Plane
```

## Architecture

- **Envoy** — transparent TCP proxy with L7 protocol filters (HTTP, Redis, Kafka, MySQL, MongoDB)
- **iptables** — redirects outbound traffic to Envoy without touching service code
- **Control Plane** — single REST API to configure mock rules across all protocols

## Quick Start

```bash
# Coming soon
```

## Mock Rule Example

```json
POST /mocks
{
  "protocol": "redis",
  "match": "GET user:*",
  "response": "{\"id\": 1, \"name\": \"fake-user\"}"
}

POST /mocks
{
  "protocol": "http",
  "match": { "method": "POST", "path": "/v1/charge" },
  "response": { "status": 200, "body": "{\"id\": \"ch_fake123\"}" }
}
```

## Supported Protocols (Roadmap)

| Protocol   | Status      | Backend              |
|------------|-------------|----------------------|
| HTTP/gRPC  | Planned     | WireMock / Microcks  |
| Redis      | Planned     | miniredis            |
| RabbitMQ   | Planned     | rabbitmq-mock        |
| Kafka      | Planned     | Microcks AsyncAPI    |
| MySQL      | Planned     | test container       |
| Oracle     | Planned     | TestContainers       |

## Inspiration

The name comes from the Italian football term **regista** — a deep-lying playmaker (think Andrea Pirlo) who sits in front of the defence and orchestrates play, distributing the ball to wherever it needs to go.

This proxy does the same: sits between your service and all external dependencies, directing each request to the right mock.

## License

MIT
