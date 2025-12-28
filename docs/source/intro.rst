Protobunny
==========

.. warning:: The project is in early development.

The ``protobunny`` library simplifies messaging for asynchronous tasks
by providing:

- A clean “message-first” API
- Python class generation from Protobuf messages using betterproto
- Connections facilities for backends
- Message publishing/subscribing with typed topics
- Generate and consume ``Result`` messages (success/failure + optional
  return payload)
- Protocol Buffers messages serialization/deserialization
- Support “task-like” queues (shared/competing consumers) vs broadcast
  subscriptions
- Support async and sync contexts
- Transparently serialize “JSON-like” payload fields (numpy-friendly)

Requirements
------------

- Python >= 3.10, < 3.14

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

See the `Quick example on
GitHub <https://github.com/am-flow/protobunny/blob/main/QUICK_START.md>`__
for installation and quick start guide.

Full docs are available at https://am-flow.github.io/protobunny/.

--------------

Development
-----------

Run tests
~~~~~~~~~

.. code:: bash

   make test

Integration tests (RabbitMQ required)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration tests expect RabbitMQ to be running (for example via Docker
Compose in this repo):

.. code:: bash

   docker compose up -d
   make integration-test

--------------

Future work
~~~~~~~~~~~

- Support grcp
- Support for RabbitMQ certificates (through ``pika``)
- More backends:

  - NATS
  - Kafka
  - Cloud providers (AWS SQS/SNS)

--------------

License
-------

``MIT`` Copyright (c) 2025 AM-Flow b.v.
