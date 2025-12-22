import asyncio
import time


async def async_wait(condition_func, timeout_seconds=1.0, sleep_seconds=0.1) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        res = await condition_func()
        if res:
            return True
        await asyncio.sleep(sleep_seconds)  # This yields control to the loop!
    return False


async def tear_down(event_loop):
    # Collect all tasks and cancel those that are not 'done'.
    tasks = asyncio.all_tasks(event_loop)
    tasks = [t for t in tasks if not t.done()]
    for task in tasks:
        task.cancel()
    # Wait for all tasks to complete, ignoring any CancelledErrors
    try:
        await asyncio.wait(tasks)
    except asyncio.exceptions.CancelledError:
        pass
