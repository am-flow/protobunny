Intro
=====

**Note**: The project is in early development.

The ``protobunny`` library simplifies messaging for asynchronous tasks
by providing:

-  A clean “message-first” API
-  Python class generation from Protobuf messages using betterproto
-  Connections facilities to RabbitMQ
-  Message publishing/subscribing with typed topics
-  Generate and consume ``Result`` messages (success/failure + optional
   return payload)
-  Protocol Buffers messages serialization/deserialization
-  Support “task-like” queues (shared/competing consumers) vs broadcast
   subscriptions
-  Support async and sync contexts
-  Transparently serialize “JSON-like” payload fields (numpy-friendly)

Requirements
------------

-  Python >= 3.10, < 3.13
-  RabbitMQ instance as message broker

Project scope
-------------

Protobunny is designed for teams who use messaging to coordinate work
between microservices or different python processes and want:

-  A small API surface, easy to learn and use, both async and sync
-  Typed RabbitMQ messaging
-  Consistent topic naming and routing
-  Builtin task queue semantics and result messages
-  Transparent handling of JSON-like payload fields as plain
   dictionaries/lists
-  Optional validation of required fields
-  Builtin logging service

Future work
~~~~~~~~~~~

-  Support gRCP
-  Support for RabbitMQ certificates (through ``pika``)
-  More backends:

   -  multiprocessing.Queue or queue.Queue for simple local scenarios
   -  Mosquitto
   -  Redis
   -  NATS
   -  Cloud providers (AWS SQS/SNS)


License
-------

``MIT`` Copyright (c) 2025 AM-Flow b.v.
