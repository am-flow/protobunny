# Protobunny

::: {warning}
**Warning**: The project is in early development.
:::

Protobunny is the open-source evolution of [AM-Flow](https://am-flow.com)'s internal messaging library. 
While the original was purpose-built for RabbitMQ, this version has been completely re-engineered to provide a unified, 
type-safe interface for several message brokers, including Redis, NATS, and MQTT.

It simplifies messaging for asynchronous message handling by providing:

* A clean “message-first” API by using your protobuf definitions
* Message publishing/subscribing with typed topics
* Supports "task-like” queues (shared/competing consumers) vs. broadcast subscriptions
* Generate and consume `Result` messages (success/failure + optional return payload)
* Transparent messages serialization/deserialization
* Transparently serialize/deserialize custom "JSON-like" payload fields (numpy-friendly)
* Support async and sync contexts

Supported backends in the current version are:

- RabbitMQ
- Redis
- NATS
- Mosquitto
- Python "backend" with Queue/asyncio.Queue for local in-processing testing

::: {note}
**Note**: Protobunny handles backend-specific logic internally to provide a consistent experience and a lean interface. 
Direct access to the internal NATS or Redis clients is intentionally restricted. 
If your project depends on specialized backend parameters not covered by our API, you may find the abstraction too restrictive.
:::


## Minimal requirements

- Python >= 3.10 <=3.13
- Core Dependencies: betterproto 2.0.0b7, grpcio-tools>=1.62.0
- Backend Drivers (Optional based on your usage):
  - NATS: nats-py (Requires NATS Server v2.10+ for full JetStream support).
  - Redis: redis (Requires Redis Server v6.2+ for Stream support).
  - RabbitMQ: aio-pika
  - Mosquitto: aiomqtt


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
* **Backend Agnostic**: You can choose between RabbitMQ, Redis, NATS, and Mosquitto. Python for local testing.
* **Sync & Async**: Support for both `asyncio` and traditional synchronous workloads.
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

---

## Usage

See the [Quick example on GitHub](https://github.com/am-flow/protobunny/blob/main/QUICK_START.md) or on the [docs site](https://am-flow.github.io/protobunny/quickstart.html).

Documentation home page: [https://am-flow.github.io/protobunny/](https://am-flow.github.io/protobunny/).

---
### Roadmap

- [x] **Core Support**: Redis, RabbitMQ, Mosquitto.
- [x] **Semantic Patterns**: Automatic `tasks` package routing.
- [x] **Arbistrary dictionary parsing**: Transparently parse JSON-like fields as dictionaries/lists by using protobunny JsonContent type.
- [x] **Result workflow**: Subscribe to results topics and receive protobunny `Result` messages produced by your callbacks.
- [x] **Cloud-Native**: NATS (Core & JetStream) integration.
- [ ] **Cloud Providers**: AWS (SQS/SNS) and GCP Pub/Sub.
- [ ] **More backends**: Kafka support.

---

## License
`MIT`
Copyright (c) 2026 AM-Flow b.v.
