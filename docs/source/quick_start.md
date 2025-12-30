# Quick start guide

This examples are for sync mode.
For async mode, import protobunny with `from protobunny import asyncio as pb`.

For a full example, using FastAPI, Redis, and protobunny, see [this repo](https://github.com/domeniconappo/fastapi-protobunny-example.git).


## Setup

### pyproject.toml
Add `protobunny` to your `pyproject.toml` dependencies:

```shell
uv add protobunny[rabbitmq, numpy]
# or
poetry add protobunny
```

You can also add it manually to pyproject.toml dependencies:

```toml
dependencies = [
  "protobunny[rabbitmq, numpy]>=0.1.0",
  # your other dependencies ...
]
```

Configure the library in pyproject.toml:
```toml
[tool.protobunny]
messages-directory = "messages"
messages-prefix = "acme"
generated-package-name = "mymessagelib.codegen"
mode = "async"  # or "sync"
backend = "rabbitmq"  #  available backends are ['rabbitmq', 'redis', 'mosquitto', 'python']
```

### Install the library with `uv`, `poetry` or `pip`
```bash
uv lock --prerelease=allow  # or poetry lock
uv sync  # or poetry sync/install
```

### Backend connection

Protobunny connects to the broker (e.g. `RabbitMQ`) by reading environment variables (`RABBITMQ_URL`).

```shell
# export these variables

RABBITMQ_HOST=localhost 
RABBITMQ_PORT=5672 
RABBITMQ_USER=guest 
RABBITMQ_PASS=guest 
RABBITMQ_VHOST=/test
```

For other backends, replace `RABBITMQ_` prefix with the backend name uppercase (e.g. `REDIS_HOST`).
If you are using the `python` backend, you don't need to set any environment variables.

Available backends are:
- RabbitMQ
- Redis
- Mosquitto
- Python for local testing (in-process)

- For docker-compose or pipelines yaml:
```yaml
env:
  RABBITMQ_HOST: localhost
  RABBITMQ_PORT: 5672
  RABBITMQ_USER: guest
  RABBITMQ_PASS: guest
  RABBITMQ_VHOST: /test
```

---

## Quick example

### Create a folder in your project with your protobuf messages
```shell
mkdir messages
mkdir messages/acme
mkdir messages/acme/tests
# etc.
```
A message that uses JSON-like fields can look like this:
```protobuf
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
```
### Generate your message library with `protobunny`
The library comes with a `protoc` wrapper that generates Python code from your protobuf messages 
and executes a postcompilation step to manipulate the generated code. 

```shell
protobunny generate
```

In `mymessagelib/codegen` you should see the generated message classes, mirroring the `package` declaration in your protobuf files.

If you need to generate the classes in another package (e.g. for tests), you can pass the `--python_betterproto_out` option:

```shell
protobunny generate -I messages --python_betterproto_out=tests tests/**/*.proto tests/*.proto
```

The following examples are for sync mode and can run from the python shell.
To use the async mode, import protobunny with `from protobunny import asyncio as pb`.

### Subscribe to a message
```python
import protobunny as pb
import mymessagelib as mml
def on_message(message: mml.tests.TestMessage) -> None:
    print("Got:", message)

pb.subscribe(mml.tests.TestMessage, on_message)
# Prints 
# 'Got: TestMessage(content="hello", number=1, data={"test": "test"}, detail=None)' 
# when a message is received
```
### Publish a message

The following code can run in another process or thread and publishes a message to the topic `acme.test.TestMessage`.
```python
import protobunny as pb
import mymessagelib as mml
msg =  mml.tests.TestMessage(content="hello", number=1, data={"test": "test"})
pb.publish(msg)
```

## Task-style queues
All messages that are under a protobuffer `tasks` package are treated as shared queues.

```protobuf
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
```

If a message is treated as a "task queue" message by the library conventions, 
`subscribe` will use a **shared queue** (multiple workers consuming messages from one queue).
The load is distributed among workers (competing consumers).

```python
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
```

You can also introspect/manage an underlying shared queue:
```python

import protobunny as pb
import mymessagelib as mml

queue = pb.get_queue(mml.main.tasks.TaskMessage)

# Only shared queues can be purged and counted
count = queue.get_message_count()
print("Queued:", count)
queue.purge()
```
---

## Results workflow

### Create and publish a result
```python
import protobunny as pb
import mymessagelib as mml

source = mml.tests.TestMessage(content="hello", number=1)

# create a result message from the source message
result = source.make_result(return_value={"ok": True})
# publish the result
pb.publish_result(result)
```

### Subscribe to results for a message type
```python

import protobunny as pb
import mymessagelib as mml

def on_result(res: pb.results.Result) -> None:
    print("Result for:", res.source)
    print("Return code:", res.return_code)
    print("Return value:", res.return_value)
    print("Error:", res.error)

pb.subscribe_results(mml.tests.TestMessage, on_result)
```

---

## JSON-like content fields
Protobuf supports maps and lists as message fields.
Maps can't have arbitrary structures: the values of a map must be of the same type.

Protobunny adds a layer over protobuf to carry arbitrary structured payloads (dicts/lists), 
by supporting transparent conversion so you can work with normal Python structures:

- Serialize: dictionaries/lists are encoded into the message field
- Deserialize: those fields come back as Python structures

This is particularly useful for metrics, metadata, and structured return values in results.

Example:
The TaskMessage above has a `options` field that can carry arbitrary JSON-like payload.

```python
import mymessagelib as mml

msg = mml.tests.TaskMessage(content="test1", options={"test":"Test", "number_list": [1,2,3]})
serialized = bytes(msg)
print(serialized)
deserialized = mml.tests.TaskMessage.parse(serialized)
print(deserialized)
assert deserialized.options == {"test":"Test", "number_list": [1,2,3]}
```


---

## Logging / debugging

Protobunny includes a convenience subscription for logging message traffic by subscribing to a broad wildcard topic and printing JSON payloads:

```python
import protobunny as pb

def log_callback(_incoming_message, body: str) -> None:
    print(body)

pb.subscribe_logger(log_callback)
```

You can start a logger worker with:

```shell
protobunny log
```

---

If you need explicit connection lifecycle control, you can access the shared connection object:
```python
import protobunny as pb

conn = pb.connect()
if conn.is_connected():
    conn.close()
```

If you set the `generated-package-root` folder option, you might need to add the path to your `sys.path`.
You can do it conveniently by calling `config_lib` on top of your module, before importing the library:
```python
pb.config_lib() 
```

## Complete example

### Config

```toml
[project]
name = "test-project"
version = "0.1.0"
description = "Project to test protobunny"
requires-python = ">=3.10"
dependencies = [
    "protobunny[rabbitmq,redis,numpy,mosquitto] >=0.1.2a1",
]

[tool.protobunny]
messages-directory = "messages"
messages-prefix = "acme"
generated-package-name = "mymessagelib"
generated-package-root = "codegen"
backend = "rabbitmq"
mode = "async"
```

### Protobuf Messages

```protobuf
/* messages/my_message.proto */

syntax = "proto3";

package main;


message MyMessage {
  string content = 10;
  int64 number = 20;
  optional string detail=30;
}

```

```protobuf
/* messages/tasks.proto */
syntax = "proto3";
import "protobunny/commons.proto";

// Define the tasks package
package main.tasks;


message TaskMessage {
  string content = 10;
  repeated float weights = 30 [packed = true];
  repeated int64 bbox = 40 [packed = true];
  optional commons.JsonContent options=50;
}

```

### Generate betterproto classes decorated with protobunny mixins

```shell
protobunny generate
```

You should find the generated classes under `codegen/mymessagelib`.

### Python code to test the library

```python

import asyncio
import logging
import sys


import protobunny as pb
from protobunny import asyncio as pb_asyncio

# sys.path.append(pb.default_configuration.generated_package_root)
# this is needed when the python classes for your lib are generated in a subfolder 
# that is not in the namespace and it must not be treated as a package
# It's just a sys.path.append("./codegen")
pb.config_lib()

import mymessagelib as ml


logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s %(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger(__name__)
conf = pb.default_configuration


class TestLibAsync:
    async def on_message(self, message: ml.tests.TestMessage) -> None:
        log.info("Got: %s", message)
        result = message.make_result()
        await pb_asyncio.publish_result(result)

    async def on_message_results(self, result: pb.results.Result) -> None:
        log.info("Got result: %s", result)
        log.info("Source: %s", result.source)

    async def worker1(self, task: ml.main.tasks.TaskMessage) -> None:
        log.info("1- Working on: %s", task)

    async def worker2(self, task: ml.main.tasks.TaskMessage) -> None:
        log.info("2- Working on: %s", task)

    async def on_message_mymessage(self, message: ml.main.MyMessage) -> None:
        log.info("Got main message: %s", message)


    def run_forever(self):
        asyncio.run(self.main())

    def log_callback(self, incoming, body) -> None:
        log.info(f"LOG {incoming.routing_key}: {body}")

    async def main(self):

        await pb_asyncio.subscribe_logger(self.log_callback)
        await pb_asyncio.subscribe(ml.main.tasks.TaskMessage, self.worker1)
        await pb_asyncio.subscribe(ml.main.tasks.TaskMessage, self.worker2)
        await pb_asyncio.subscribe(ml.tests.TestMessage, self.on_message)
        await pb_asyncio.subscribe_results(ml.tests.TestMessage, self.on_message_results)
        await pb_asyncio.subscribe(ml.main.MyMessage, self.on_message_mymessage)

        await pb_asyncio.publish(ml.main.MyMessage(content="test"))
        await pb_asyncio.publish(ml.tests.TestMessage(number=1, content="test", data={"test": 123}))

        await pb_asyncio.publish(ml.main.tasks.TaskMessage(content="test1"))
        await pb_asyncio.publish(ml.main.tasks.TaskMessage(content="test2"))
        await pb_asyncio.publish(ml.main.tasks.TaskMessage(content="test3"))

        log.info("TEST LIB started. Press Ctrl+C to exit.")


class TestLib:
    def on_message(self, message: ml.tests.TestMessage) -> None:
        log.info("Got: %s", message)
        result = message.make_result()
        pb.publish_result(result)

    def on_message_results(self, result: pb.results.Result) -> None:
        log.info("Got result: %s", result)
        log.info("Source: %s", result.source)

    def worker1(self, task: ml.main.tasks.TaskMessage) -> None:
        log.info("1- Working on: %s", task)

    def worker2(self, task: ml.main.tasks.TaskMessage) -> None:
        log.info("2- Working on: %s", task)

    def log_callback(self, incoming, body) -> None:
        log.info(f"LOG {incoming.routing_key}: {body}")

    def main(self):
        pb.subscribe_logger(self.log_callback)
        pb.subscribe(ml.main.tasks.TaskMessage, self.worker1)
        pb.subscribe(ml.main.tasks.TaskMessage, self.worker2)
        pb.subscribe(ml.tests.TestMessage, self.on_message)
        pb.subscribe_results(ml.tests.TestMessage, self.on_message_results)
        pb.subscribe(ml.main.MyMessage, lambda x: log.info(x))

        pb.publish(ml.main.MyMessage(content="test"))
        pb.publish(ml.tests.TestMessage(number=1, content="test", data={"test": 123}))

        pb.publish(ml.main.tasks.TaskMessage(content="test1"))
        pb.publish(ml.main.tasks.TaskMessage(content="test2"))
        pb.publish(ml.main.tasks.TaskMessage(content="test3"))


if __name__ == "__main__":
    if conf.use_async:
        log.info("Using async")
        testlib = TestLibAsync()
        pb_asyncio.run_forever(testlib.main)

    else:
        log.info("Using sync")
        testlib = TestLib()
        testlib.main()
        pb.run_forever()

```

### Run the test (ensure the configured backend is running and the extra dependencies are installed)

```shell
python test_lib.py 
```

Output:


```text
[2025-12-30 01:05:23,702 INFO] __main__ - Using async
[2025-12-30 01:05:23,702 INFO] protobunny - Started. Press Ctrl+C to exit.
[2025-12-30 01:05:23,742 INFO] protobunny.asyncio.backends.rabbitmq.connection - Establishing RabbitMQ connection to 127.0.0.1:5672/%2F
[2025-12-30 01:05:23,768 INFO] protobunny.asyncio.backends.rabbitmq.connection - Successfully connected to RabbitMQ
[2025-12-30 01:05:23,772 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.#' (queue=amq_37755497ba5d47ca95956d0bef5f6ae9, shared=False)
[2025-12-30 01:05:23,775 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.main.tasks.TaskMessage' (queue=acme.main.tasks.TaskMessage, shared=True)
[2025-12-30 01:05:23,776 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.main.tasks.TaskMessage' (queue=acme.main.tasks.TaskMessage, shared=True)
[2025-12-30 01:05:23,780 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.tests.TestMessage' (queue=amq_1cf4275aecd643d6ad711c7bbf6de31d, shared=False)
[2025-12-30 01:05:23,784 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.tests.TestMessage.result' (queue=amq_a014606e624348a894965c36e2c7fd26, shared=False)
[2025-12-30 01:05:23,787 INFO] protobunny.asyncio.backends.rabbitmq.connection - Subscribing to topic 'acme.main.MyMessage' (queue=amq_0791c366494348b0be55ff095fc3e71c, shared=False)
[2025-12-30 01:05:23,789 INFO] __main__ - Got main message: MyMessage(content='test', detail=None)
[2025-12-30 01:05:23,789 INFO] __main__ - LOG acme.main.MyMessage: {"content": "test", "number": 0, "detail": null}
[2025-12-30 01:05:23,791 INFO] __main__ - Got: TestMessage(content='test', number=1, data={'test': 123}, detail=None)
[2025-12-30 01:05:23,791 INFO] protobunny.asyncio.backends - Publishing result to: acme.tests.TestMessage.result
[2025-12-30 01:05:23,792 INFO] __main__ - LOG acme.tests.TestMessage: {"content": "test", "number": 1, "data": {"test": 123}, "detail": null}
[2025-12-30 01:05:23,793 INFO] __main__ - LOG acme.main.tasks.TaskMessage: {"content": "test1", "weights": [], "bbox": [], "options": null}
[2025-12-30 01:05:23,793 INFO] __main__ - 1- Working on: TaskMessage(content='test1', options=None)
[2025-12-30 01:05:23,793 INFO] __main__ - Got result: Result(source_message=Any(type_url='mymessagelib.tests.TestMessage', value=b'R\x04test\xa0\x01\x01\xca\x01\x0f\n\r{"test": 123}'), return_code=ReturnCode.SUCCESS, error='', return_value=None)
[2025-12-30 01:05:23,794 INFO] __main__ - Source: TestMessage(content='test', number=1, data={'test': 123}, detail=None)
[2025-12-30 01:05:23,795 INFO] __main__ - LOG acme.tests.TestMessage.result: SUCCESS - {"content": "test", "number": 1, "data": {"test": 123}, "detail": null}
[2025-12-30 01:05:23,795 INFO] __main__ - 2- Working on: TaskMessage(content='test2', options=None)
[2025-12-30 01:05:23,795 INFO] __main__ - LOG acme.main.tasks.TaskMessage: {"content": "test2", "weights": [], "bbox": [], "options": null}
[2025-12-30 01:05:23,796 INFO] __main__ - 1- Working on: TaskMessage(content='test3', options=None)
[2025-12-30 01:05:23,796 INFO] __main__ - LOG acme.main.tasks.TaskMessage: {"content": "test3", "weights": [], "bbox": [], "options": null}
[2025-12-30 01:05:23,796 INFO] __main__ - TEST LIB started. Press Ctrl+C to exit.
```
