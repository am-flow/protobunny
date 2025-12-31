# Protobunny

```{warning}
Note: The project is in early development.
```

Protobunny is the open-source evolution of [AM-Flow](https://am-flow.com)'s internal messaging library. 
While the original was purpose-built for RabbitMQ, this version has been completely re-engineered to provide a unified, 
type-safe interface for several message brokers, including Redis and MQTT.

It simplifies messaging for asynchronous tasks by providing:

* A clean “message-first” API
* Python class generation from Protobuf messages using betterproto
* Connections facilities for backends
* Message publishing/subscribing with typed topics
* Support also “task-like” queues (shared/competing consumers) vs. broadcast subscriptions
* Generate and consume `Result` messages (success/failure + optional return payload)
* Transparent messages serialization/deserialization
 * Support async and sync contexts
* Transparently serialize "JSON-like" payload fields (numpy-friendly)


## Minimal requirements

- Python >= 3.10, < 3.14


## Project scope

Protobunny is designed for teams who use messaging to coordinate work between microservices or different python processes and want:

- A small API surface, easy to learn and use, both async and sync
- Typed messaging with protobuf messages as payloads
- Supports various backends by simple configuration: RabbitMQ, Redis, Mosquitto, local in-process queues
- Consistent topic naming and routing
- Builtin task queue semantics and result messages
- Transparent handling of JSON-like payload fields as plain dictionaries/lists
- Optional validation of required fields
- Builtin logging service

## Why Protobunny?

While there are many messaging libraries for Python, Protobunny is built specifically for teams that treat **Protobuf as the single source of truth**.

* **Type-Safe by Design**: Built natively for `protobuf/betterproto`.
* **Semantic Routing**: Zero-config infrastructure. Protobunny uses your Protobuf package structure to decide if a message should be broadcast (Pub/Sub) or queued (Producer/Consumer).
* **Backend Agnostic**: Write your logic once. Switch between Redis, RabbitMQ, Mosquitto, or Local Queues by changing a single variable in configuration.
* **Sync & Async**: Support for both modern `asyncio` and traditional synchronous workloads.
* **Battle-Tested**: Derived from internal libraries used in production systems at AM-Flow.
---

### Feature Comparison with some existing libraries

| Feature                | **Protobunny**           | **FastStream**          | **Celery**              |
|:-----------------------|:-------------------------|:------------------------|:------------------------|
| **Multi-Backend**      | ✅ Yes                    | ✅ Yes                   | ⚠️ (Tasks only)         |
| **Typed Protobufs**    | ✅ Native (Betterproto)   | ⚠️ Manual/Pydantic      | ❌ No                    |
| **Sync + Async**       | ✅ Yes                    | ✅ Yes                   | ❌ Sync focus            |
| **Pattern Routing**    | ✅ Auto (`tasks` pkg)     | ❌ Manual Config         | ✅ Fixed                 |
| **Framework Agnostic** | ✅ Yes                    | ⚠️ FastAPI-like focus   | ❌ Heavyweight           |


## Usage

See the [Quick example on GitHub](https://github.com/am-flow/protobunny/blob/main/QUICK_START.md) or on the [docs site](https://am-flow.github.io/protobunny/quickstart.html).

Documentation home page: [https://am-flow.github.io/protobunny/](https://am-flow.github.io/protobunny/).

---
### Roadmap

- [x] **Core Support**: Redis, RabbitMQ, Mosquitto.
- [x] **Semantic Patterns**: Automatic `tasks` package routing.
- [x] **Arbistrary dictionary parsing**: Transparently parse JSON-like fields as dictionaries/lists by using protobunny JsonContent type.
- [x] **Result workflow**: Subscribe to results topics and receive protobunny `Result` messages produced by your callbacks.
- [ ] **Cloud-Native**: NATS (Core & JetStream) integration.
- [ ] **Cloud Providers**: AWS (SQS/SNS) and GCP Pub/Sub.
- [ ] **More backends**: Kafka support.

---

## License
`MIT`
Copyright (c) 2026 AM-Flow b.v.
