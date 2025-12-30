Protobunny
==========

.. warning:: The project is in early development.

Protobunny is the open-source evolution of
`AM-Flow <https://am-flow.com>`__\ ’s internal messaging library. While
the original was purpose-built for RabbitMQ, this version has been
completely re-engineered to provide a unified, type-safe interface for
any message broker, including Redis and MQTT.

It simplifies messaging for asynchronous tasks by providing:

- A clean “message-first” API
- Python class generation from Protobuf messages using betterproto
- Connections facilities for backends
- Message publishing/subscribing with typed topics
- Support also “task-like” queues (shared/competing consumers)
  vs. broadcast subscriptions
- Generate and consume ``Result`` messages (success/failure + optional
  return payload)
- Transparent messages serialization/deserialization
- Support async and sync contexts
- Transparently serialize “JSON-like” payload fields (numpy-friendly)

Requirements
------------

- Python >= 3.10, < 3.14
- Backend message broker (e.g. RabbitMQ)

Project scope
-------------

Protobunny is designed for teams who use messaging to coordinate work
between microservices or different python processes and want:

- A small API surface, easy to learn and use, both async and sync
- Typed messaging with protobuf messages as payloads
- Supports various backends by simple configuration: RabbitMQ, Redis,
  Mosquitto, local in-process queues
- Consistent topic naming and routing
- Builtin task queue semantics and result messages
- Transparent handling of JSON-like payload fields as plain
  dictionaries/lists
- Optional validation of required fields
- Builtin logging service

--------------

Usage
-----

See the `Quick start guide <quick_start.html>`__.


--------------


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   intro
   quick_start
   concepts
   api
