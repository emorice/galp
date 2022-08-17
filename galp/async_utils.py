"""
Asyncio recipes
"""

import asyncio
from contextlib import asynccontextmanager, AsyncExitStack

@asynccontextmanager
async def background(coroutine):
    """
    Runs a coroutine in background and cancels on exit
    """
    task = asyncio.create_task(coroutine)
    try:
        yield task
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

async def run(*coroutines):
    """
    Runs several coroutines in parallel.

    Return when any of them finishes in any way (return, exception,
    cancellation). All other coroutines are then cancelled and awaited, even if
    this results in other exceptions being thrown.
    """
    tasks = []
    async with AsyncExitStack() as stack:
        for coroutine in coroutines:
            tasks.append(
                    await stack.enter_async_context(
                        background(coroutine)
                        )
                    )
        await asyncio.sleep(.2)
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
