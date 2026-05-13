#!/bin/sh
# envoy/iptables.sh
# Run as root before Envoy starts.
# Sets up outbound traffic redirection for the shared network namespace.
set -e

# Envoy runs as UID 1337. Exclude its own outbound traffic to prevent redirect loops.
iptables -t nat -A OUTPUT -m owner --uid-owner 1337 -j RETURN

# Redirect per-protocol outbound ports to Envoy listeners.
iptables -t nat -A OUTPUT -p tcp --dport 80   -j REDIRECT --to-port 15001
iptables -t nat -A OUTPUT -p tcp --dport 443  -j REDIRECT --to-port 15002
iptables -t nat -A OUTPUT -p tcp --dport 6379 -j REDIRECT --to-port 15003
# Ports below enabled in Phase 2 and 3:
# iptables -t nat -A OUTPUT -p tcp --dport 9092 -j REDIRECT --to-port 15004
# iptables -t nat -A OUTPUT -p tcp --dport 3306 -j REDIRECT --to-port 15005
# iptables -t nat -A OUTPUT -p tcp --dport 5672 -j REDIRECT --to-port 15006

echo "iptables rules applied"
