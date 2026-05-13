#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== AMQP RPC Example ==="
echo "Mirage consumes from 'profile_service' queue and replies to replyTo."
echo "Requires Python with pika: pip install pika"

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{
    "protocol": "amqp-rpc",
    "match": {
      "queue": "profile_service",
      "properties.type": "GET_PROFILE"
    },
    "response": "{\"userId\":1,\"name\":\"ada\",\"phone\":\"0901234567\"}",
    "priority": 1
  }' | jq .

echo "--- Sending RPC request via Python ---"
if command -v python3 &>/dev/null && python3 -c "import pika" 2>/dev/null; then
python3 - <<'PYEOF'
import pika, uuid, threading

AMQP_URL = "amqp://guest:guest@localhost:5673/"
conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
ch = conn.channel()

# Declare a reply queue
result = ch.queue_declare(queue='', exclusive=True)
reply_queue = result.method.queue

corr_id = str(uuid.uuid4())
response = None
event = threading.Event()

def on_response(ch, method, props, body):
    global response
    if props.correlation_id == corr_id:
        response = body.decode()
        event.set()

ch.basic_consume(queue=reply_queue, on_message_callback=on_response, auto_ack=True)

# Publish RPC request
ch.basic_publish(
    exchange='',
    routing_key='profile_service',
    properties=pika.BasicProperties(
        reply_to=reply_queue,
        correlation_id=corr_id,
        type='GET_PROFILE',
        content_type='application/json',
    ),
    body='{"userId": 1}'
)
print(f"Sent RPC request (correlationId: {corr_id})")

# Wait for reply (5s timeout)
event.wait(timeout=5)
if response:
    print(f"Received reply: {response}")
else:
    print("No reply received within 5 seconds")

conn.close()
PYEOF
else
  echo "Python3 + pika not available."
  echo "Install with: pip install pika"
  echo ""
  echo "Manual test: publish to queue 'profile_service' with:"
  echo "  properties.type = GET_PROFILE"
  echo "  reply_to = <your-reply-queue>"
  echo "  correlation_id = <any-uuid>"
  echo "Mirage will respond to your reply queue with: {\"userId\":1,\"name\":\"ada\"}"
fi

echo ""
echo "--- Traffic (intercepted RPC calls) ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
