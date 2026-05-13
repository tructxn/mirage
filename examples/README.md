# Mirage Examples

One script per protocol. Requires the full stack running:

```bash
cd deploy && docker compose up -d
```

Then run any example:

```bash
bash examples/http/example.sh
bash examples/redis/example.sh
bash examples/kafka/example.sh
bash examples/mysql/example.sh
bash examples/amqp/example.sh
bash examples/amqp-rpc/example.sh
```

Or run all:

```bash
bash examples/run-all.sh
```
