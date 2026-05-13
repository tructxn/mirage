# Architecture

## Overview

```
[Your Service]
      │
      │  iptables REDIRECT (per port)
      ▼
[Envoy Sidecar]
      │
      ├─ port 80/443  ──► HTTP L7 filter  ──► Control Plane :8080
      ├─ port 6379    ──► redis_proxy     ──► Control Plane :6379
      ├─ port 5672    ──► tcp_proxy       ──► Control Plane :5672
      └─ port 9092    ──► kafka_broker    ──► Control Plane :9092
```

## Components

### 1. iptables Redirect
Intercepts all outbound TCP before it leaves the host/container.
No code changes required in the service under test.

```bash
# Example: redirect Redis traffic to Envoy
iptables -t nat -A OUTPUT -p tcp --dport 6379 -j REDIRECT --to-port 15001
```

### 2. Envoy Proxy
Protocol-aware L7 proxy. Understands wire format of each protocol.

Key filters used:
- `http_connection_manager` — HTTP/1.1, HTTP/2, gRPC
- `redis_proxy` — Redis RESP protocol
- `kafka_broker` — Kafka wire protocol
- `mysql_proxy` — MySQL protocol
- `tcp_proxy` — fallback for raw TCP (AMQP, Oracle TNS)

### 3. Control Plane (to build)
Single REST API for configuring mock rules across all protocols.
Pushes rules to the appropriate mock backend.

```
POST   /mocks          — create a mock rule
GET    /mocks          — list all rules
DELETE /mocks/:id      — remove a rule
POST   /mocks/reset    — clear all rules
GET    /traffic        — inspect intercepted traffic
```

## Protocol Support

### HTTP
Envoy HTTP filter chain → forward to WireMock or built-in mock handler.
Full request matching: method, path, headers, body.

### Redis
Envoy `redis_proxy` filter parses RESP protocol.
Commands routed to miniredis-compatible mock backend.
Control plane can seed key-value pairs or define command-level responses.

### RabbitMQ (AMQP)
Envoy `tcp_proxy` (no native AMQP filter).
Backend: rabbitmq-mock (Java) or real RabbitMQ in lightweight container.

### Kafka
Envoy `kafka_broker` filter.
Backend: Microcks AsyncAPI or embedded Kafka (testcontainers-kafka).

### Oracle
No Envoy filter for Oracle TNS protocol.
Backend: TestContainers Oracle Free.
iptables redirect to containerized Oracle on localhost.

## Deployment Modes

### Local Dev (docker-compose)
All components run in docker-compose.
Service HTTP_PROXY / env vars point to Envoy.

### Sidecar (Kubernetes)
Envoy runs as sidecar container.
Init container sets iptables rules (same pattern as Istio).

## References
- [Envoy redis_proxy filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/listeners/network_filters/redis_proxy_filter)
- [Envoy kafka_broker filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/listeners/network_filters/kafka_broker_filter)
- [Microcks](https://microcks.io/)
- [miniredis](https://github.com/alicebob/miniredis)
- [rabbitmq-mock](https://github.com/fridujo/rabbitmq-mock)
- [Grab Engineering: Loki — HTTP + TCP mock server](https://engineering.grab.com/loki-dynamic-mock-server-http-tcp-testing)
