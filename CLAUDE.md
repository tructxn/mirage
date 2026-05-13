# Regista — Claude Instructions

## Project Overview

**Regista** is a universal mock proxy for E2E testing. Like Andrea Pirlo's regista role — sits deep, orchestrates everything, distributes to wherever it needs to go.

Architecture: `iptables redirect → Envoy (protocol-aware proxy) → Control Plane (mock rules API)`

## Stack

- **Envoy** — transparent TCP proxy, L7 protocol filters (HTTP, Redis, Kafka, MySQL)
- **Control Plane** — REST API to configure mock rules (language TBD, likely Go or Java)
- **Protocol Backends** — miniredis, rabbitmq-mock, WireMock, TestContainers as needed
- **Deploy** — docker-compose (local), Kubernetes sidecar (prod)

## Folder Structure

```
envoy/          — Envoy config (envoy.yaml, listeners, filters)
control-plane/  — Mock rules API server
docs/           — Architecture, ADRs, protocol notes
deploy/         — docker-compose.yml, k8s manifests, iptables scripts
```

## Key Design Decisions

- **No code changes** to the service under test — all interception is at network level
- **One control plane API** for all protocols — unified mock rule format
- **Envoy is the router** — it parses protocols, not the control plane
- **Protocol backends are thin** — control plane pushes config to them, they just serve

## Envoy Protocol Filter Mapping

| Protocol  | Port | Envoy Filter               | Backend              |
|-----------|------|----------------------------|----------------------|
| HTTP/gRPC | 80/443 | `http_connection_manager` | control-plane :8080  |
| Redis     | 6379 | `redis_proxy`              | miniredis :6380      |
| Kafka     | 9092 | `kafka_broker`             | mock backend :9093   |
| MySQL     | 3306 | `mysql_proxy`              | test DB :3307        |
| AMQP      | 5672 | `tcp_proxy` (raw)          | rabbitmq-mock :5673  |
| Oracle    | 1521 | `tcp_proxy` (raw)          | TestContainers :1522 |

## Mock Rule Format (target API)

```json
POST /mocks
{
  "protocol": "redis|http|kafka|amqp",
  "match": { ... },
  "response": { ... }
}
```

## Development Guidelines

- Always test with a real service pointing at the proxy, not unit tests of the proxy itself
- When adding a new protocol: (1) add Envoy filter config, (2) add backend, (3) add control plane rule translator
- Envoy config changes → restart Envoy container (no hot reload needed for now)
- Keep docker-compose as the primary local dev experience

## References

- [Envoy redis_proxy](https://www.envoyproxy.io/docs/envoy/latest/configuration/listeners/network_filters/redis_proxy_filter)
- [Envoy kafka_broker](https://www.envoyproxy.io/docs/envoy/latest/configuration/listeners/network_filters/kafka_broker_filter)
- [miniredis](https://github.com/alicebob/miniredis)
- [rabbitmq-mock](https://github.com/fridujo/rabbitmq-mock)
- [Microcks](https://microcks.io/)
- [Grab Loki — inspiration](https://engineering.grab.com/loki-dynamic-mock-server-http-tcp-testing)
- [Traffic Parrot — commercial equivalent](https://trafficparrot.com/)
