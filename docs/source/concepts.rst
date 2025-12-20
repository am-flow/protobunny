Concepts
--------

Topics
~~~~~~

Every message class is associated with a **topic string**.

Publishing sends your message to that topic; subscribing binds a queue
to the same topic pattern.

Typical patterns:

-  Exact topic: ``acme.some.Package.Message``
-  Wildcards: e.g. ``acme.#`` with `pb.subscribe(acme, my_callback)` (subscribe to everything under ``acme.``)
-  Package-level subscription: subscribe to a module/package to receive
   multiple message types eg. ``acme.some``

Shared “task” queues vs broadcast subscriptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Protobunny supports two common consumption models:

-  **Broadcast / pub-sub**: each subscriber gets its own queue and
   receives its own copy of each message.
-  **Shared / worker queue**: multiple consumers share one durable
   queue; messages are distributed among them (competing consumers).

Which one is used depends on the message/topic type and how the queue is
defined by the library conventions.

Results (reply-style messages)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For workflows that need an outcome, Protobunny supports publishing and
subscribing to **result topics** associated with a source message.

A result typically contains:

-  The original source message (embedded)
-  A return code (success/failure)
-  Optional ``return_value`` payload (often JSON-like)
-  Optional error details

--------------

Task-style queues
-----------------

All messages that are under a ``tasks`` package are treated as shared
queues.

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

   pb.subscribe_sync(mml.main.tasks.TaskMessage, worker1)
   pb.subscribe_sync(mml.main.tasks.TaskMessage, worker2)
   pb.publish_sync(mml.main.tasks.TaskMessage(content="test1"))
   pb.publish_sync(mml.main.tasks.TaskMessage(content="test2"))
   pb.publish_sync(mml.main.tasks.TaskMessage(content="test3"))

You can also introspect/manage an underlying shared queue:

.. code:: python


   import protobunny as pb
   import mymessagelib as mml

   queue = pb.get_queue(mml.main.tasks.TaskMessage)

   # Only shared queues can be purged and counted
   count = queue.get_message_count()
   print("Queued:", count)
   queue.purge()
