# Recipes

These examples are for sync context.
For async, imports the asyncio module and the logic remains the same, just with `async/await`.

```python
from protobunny import asyncio as pb
```

## Subscribe to a queue

To subscribe to a specific message type, use the `subscribe` method. This creates an exclusive queue by default (one consumer per queue instance).

```python
import protobunny as pb
import mymessagelib as mml


def on_message(message: mml.tests.TestMessage) -> None:
    print("Received:", message.content)


# Subscribe to the message class
pb.subscribe(mml.tests.TestMessage, on_message)

# Block and wait for messages
pb.run_forever()
```

For the async version, `run_forever` accepts your main async method as coroutine, that will contain the `await pb.subscribe` calls.

```python
from protobunny import asyncio as pb
import mymessagelib as mml


async def on_message(message: mml.tests.TestMessage) -> None:
    print("Received:", message.content)


async def main():
    await pb.subscribe(mml.tests.TestMessage, on_message)


pb.run_forever(main)
```


## Subscribe a task worker to a shared topic

Protobunny treats any message defined within a `.tasks` package as a task. 
Subscribing to these messages uses a shared queue, allowing multiple workers to balance the load (competing consumers).

```python
import protobunny as pb
import mymessagelib.main.tasks as tasks

def worker(task: tasks.TaskMessage) -> None:
    print("Processing task:", task.content)
    # Perform logic here...

# Multiple instances of this script will share the load from the same queue
pb.subscribe(tasks.TaskMessage, worker)
pb.run_forever()
```

## Publish

Publishing is straightforward. Protobunny automatically determines the correct topic and queue routing based on the message class.

```python
import protobunny as pb
import mymessagelib as mml

# Create the message instance
msg = mml.tests.TestMessage(content="Hello World", number=42)

# Publish it
pb.publish(msg)
```

## Results workflow

The results workflow allows you to send and receive feedback for a specific message, using the built-in `Result` message type.

### Publishing a Result
Inside a message handler, you can create and publish a result tied to the source message.

```python
def on_message(message: mml.tests.TestMessage) -> None:
    # ... process message ...

    # Create a result from the source message
    result = message.make_result(
        return_value={"status": "success", "processed_at": "12:00"}
    )
    pb.publish_result(result)
```

### Subscribing to Results
To listen for results of a specific message type:

```python
def on_result(res: pb.results.Result) -> None:
    # Access the original message that triggered this result
    print("Source message:", res.source)
    print("Data:", res.return_value)

pb.subscribe_results(mml.tests.TestMessage, on_result)
```

## Requeuing

If message processing fails and you want the broker to requeue it for another attempt, raise the `RequeueMessage` exception.

```python
from protobunny import RequeueMessage
import mymessagelib as mml

def on_message(message: mml.tests.TestMessage) -> None:
    try:
        # Attempt processing
        do_work(message)
    except Exception:
        # This tells the backend to put the message back in the queue
        raise RequeueMessage("Service busy, retrying...")
```
