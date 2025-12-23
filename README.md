<div style="width: 50%; margin: 0 auto;">
  <img src="https://raw.githubusercontent.com/am-flow/protobunny/main/images/logo.png" alt="protobunny logo">
</div>

---

# Protobunny

Note: The project is in early development.

The `protobunny` library simplifies messaging for asynchronous tasks by providing:

* A clean “message-first” API
* Python class generation from Protobuf messages using betterproto
* Connections facilities to RabbitMQ
* Message publishing/subscribing with typed topics
* Generate and consume `Result` messages (success/failure + optional return payload)
* Protocol Buffers messages serialization/deserialization
* Support “task-like” queues (shared/competing consumers) vs broadcast subscriptions 
* Support async and sync contexts
* Transparently serialize "JSON-like" payload fields (numpy-friendly)

## Requirements

- Python >= 3.10, < 3.13
- A running RabbitMQ instance (v4.0+ is preferred)

## Project scope

Protobunny is designed for teams who use messaging to coordinate work between microservices or different python processes and want:

- A small API surface, easy to learn and use, both async and sync
- Typed RabbitMQ and Redis messaging with protobuf messages as payloads
- Consistent topic naming and routing
- Builtin task queue semantics and result messages
- Transparent handling of JSON-like payload fields as plain dictionaries/lists
- Optional validation of required fields
- Builtin logging service
- Alternative configurable backends (e.g. `rabbitmq`, `redis` or `python` for local in-process queues)
---

## Usage

See the [Quick example on GitHub](https://github.com/am-flow/protobunny/blob/main/QUICK_START.md) for installation and quick start guide.

Full docs are available at [https://am-flow.github.io/protobunny/](https://am-flow.github.io/protobunny/).

---

## Development

### Run tests
```bash
make test
```

### Integration tests (RabbitMQ required)

Integration tests expect RabbitMQ to be running (for example via Docker Compose in this repo):
```bash
docker compose up -d
make integration-test
```
---

### Future work

- Support grcp
- Support for RabbitMQ certificates (through `pika`)
- More backends:
  - Mosquitto
  - NATS
  - Kafka
  - Cloud providers (AWS SQS/SNS)
---

## License
`MIT`
Copyright (c) 2025 AM-Flow b.v.
