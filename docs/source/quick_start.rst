Quick start guide
=================

Setup
-----

pyproject.toml
~~~~~~~~~~~~~~

Add ``protobunny`` to your ``pyproject.toml`` dependencies:

.. code:: shell

   uv add protobunny[rabbitmq, numpy]
   # or
   poetry add protobunny

You can also add it manually to pyproject.toml dependencies:

.. code:: toml

   dependencies = [
     "protobunny[rabbitmq, numpy]>=0.1.0",
     # your other dependencies ...
   ]

Configure the library in pyproject.toml:

.. code:: toml

   [tool.protobunny]
   messages-directory = "messages"
   messages-prefix = "acme"
   generated-package-name = "mymessagelib.codegen"
   mode = "async"  # or "sync"
   backend = "rabbitmq"  #  available backends are ['rabbitmq', 'redis', 'mosquitto', 'python']

Install the library with ``uv``, ``poetry`` or ``pip``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   uv lock --prerelease=allow  # or poetry lock
   uv sync  # or poetry sync/install

RabbitMQ connection
~~~~~~~~~~~~~~~~~~~

Protobunny connects to RabbitMQ by reading environment variables.

.. code:: shell

   export RABBITMQ_HOST=localhost RABBITMQ_PORT=5672 RABBITMQ_USER=guest RABBITMQ_PASS=guest RABBITMQ_VHOST=/test

For other backends, replace ``RABBITMQ_`` prefix with the backend name
(e.g. ``REDIS_HOST``). If you are using the ``python`` backend, you
don’t need to set any environment variables.

For docker-compose or pipelines yaml:

.. code:: yaml

   env:
     RABBITMQ_HOST: localhost
     RABBITMQ_PORT: 5672
     RABBITMQ_USER: guest
     RABBITMQ_PASS: guest
     RABBITMQ_VHOST: /test

--------------

Quick example
-------------

Create a folder in your project with your protobuf messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: shell

   mkdir messages
   mkdir messages/acme
   mkdir messages/acme/tests
   # etc.

A message that uses JSON-like fields can look like this:

.. code:: protobuf

   /*test.proto*/
   syntax = "proto3";
   import "protobunny/commons.proto";

   package acme.tests;

   message TestMessage {
     string content = 10;
     int64 number = 20;
     commons.JsonContent data = 25;
     /* Field with JSON-like content */
     optional string detail=30;
     /* Optional field */
   }

Generate your message library with ``protobunny``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The library comes with a ``protoc`` wrapper that generates Python code
from your protobuf messages and executes a postcompilation step to
manipulate the generated code.

.. code:: shell

   protobunny generate

In ``mymessagelib/codegen`` you should see the generated message
classes, mirroring the ``package`` declaration in your protobuf files.

If you need to generate the classes in another package (e.g. for tests),
you can pass the ``--python_betterproto_out`` option:

.. code:: shell

   protobunny generate -I messages --python_betterproto_out=tests tests/**/*.proto tests/*.proto

The following examples are for sync mode and can run from the python
shell. To use the async mode, import protobunny with
``from protobunny import asyncio as pb``.

Subscribe to a message
~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

   import protobunny as pb
   import mymessagelib as mml
   def on_message(message: mml.tests.TestMessage) -> None:
       print("Got:", message)

   pb.subscribe(mml.tests.TestMessage, on_message)
   # Prints 
   # 'Got: TestMessage(content="hello", number=1, data={"test": "test"}, detail=None)' 
   # when a message is received

Publish a message
~~~~~~~~~~~~~~~~~

The following code can run in another process or thread and publishes a
message to the topic ``acme.test.TestMessage``.

.. code:: python

   import protobunny as pb
   import mymessagelib as mml
   msg =  mml.tests.TestMessage(content="hello", number=1, data={"test": "test"})
   pb.publish(msg)

Task-style queues
-----------------

All messages that are under a protobuffer ``tasks`` package are treated
as shared queues.

.. code:: protobuf

   /*
   This .proto file contains protobuf message definitions for testing tasks
   */
   syntax = "proto3";
   import "protobunny/commons.proto";

   // Define the tasks package
   package tests.tasks;


   message TaskMessage {
     string content = 10;
     repeated float weights = 30 [packed = true];
     repeated int64 bbox = 40 [packed = true];
     optional commons.JsonContent options=50;
   }

If a message is treated as a “task queue” message by the library
conventions, ``subscribe`` will use a **shared queue** (multiple workers
consuming messages from one queue). The load is distributed among
workers (competing consumers).

.. code:: python

   import protobunny as pb
   import mymessagelib as mml

   def worker1(task: mml.main.tasks.TaskMessage) -> None:
       print("1- Working on:", task)

   def worker2(task: mml.main.tasks.TaskMessage) -> None:
       print("2- Working on:", task)

   pb.subscribe(mml.main.tasks.TaskMessage, worker1)
   pb.subscribe(mml.main.tasks.TaskMessage, worker2)
   pb.publish(mml.main.tasks.TaskMessage(content="test1"))
   pb.publish(mml.main.tasks.TaskMessage(content="test2"))
   pb.publish(mml.main.tasks.TaskMessage(content="test3"))

You can also introspect/manage an underlying shared queue:

.. code:: python


   import protobunny as pb
   import mymessagelib as mml

   queue = pb.get_queue(mml.main.tasks.TaskMessage)

   # Only shared queues can be purged and counted
   count = queue.get_message_count()
   print("Queued:", count)
   queue.purge()

--------------

Results workflow
----------------

Create and publish a result
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

   import protobunny as pb
   import mymessagelib as mml

   source = mml.tests.TestMessage(content="hello", number=1)

   # create a result message from the source message
   result = source.make_result(return_value={"ok": True})
   # publish the result
   pb.publish_result(result)

Subscribe to results for a message type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python


   import protobunny as pb
   import mymessagelib as mml

   def on_result(res: pb.results.Result) -> None:
       print("Result for:", res.source)
       print("Return code:", res.return_code)
       print("Return value:", res.return_value)
       print("Error:", res.error)

   pb.subscribe_results(mml.tests.TestMessage, on_result)

--------------

JSON-like content fields
------------------------

Protobuf supports maps and lists as message fields. Maps can’t have
arbitrary structures: the values of a map must be of the same type.

Protobunny adds a layer over protobuf to carry arbitrary structured
payloads (dicts/lists), by supporting transparent conversion so you can
work with normal Python structures:

- Serialize: dictionaries/lists are encoded into the message field
- Deserialize: those fields come back as Python structures

This is particularly useful for metrics, metadata, and structured return
values in results.

Example: The TaskMessage above has a ``options`` field that can carry
arbitrary JSON-like payload.

.. code:: python

   import mymessagelib as mml

   msg = mml.tests.TaskMessage(content="test1", options={"test":"Test", "number_list": [1,2,3]})
   serialized = bytes(msg)
   print(serialized)
   deserialized = mml.tests.TaskMessage.parse(serialized)
   print(deserialized)
   assert deserialized.options == {"test":"Test", "number_list": [1,2,3]}

--------------

Logging / debugging
-------------------

Protobunny includes a convenience subscription for logging message
traffic by subscribing to a broad wildcard topic and printing JSON
payloads:

.. code:: python

   import protobunny as pb

   def log_callback(_incoming_message, body: str) -> None:
       print(body)

   pb.subscribe_logger(log_callback)

You can start a logger worker with:

.. code:: shell

   protobunny log

--------------

If you need explicit connection lifecycle control, you can access the
shared connection object:

.. code:: python

   import protobunny as pb

   conn = pb.get_connection()
